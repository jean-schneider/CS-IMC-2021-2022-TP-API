from distutils.command import build
import logging
from py2neo import Graph
import os
import pyodbc as pyodbc
import azure.functions as func

def getRequestAttribute(req: func.HttpRequest, param):
    param = req.params.get(param)
    if not param:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            param = req_body.get(param)
    if param:
        param = param.replace('"','')
    return param

def buildNeo4jQuery(name, genre, role):
    query = "MATCH (n:Name)-[r]->(t:Title)-[s]->(g:Genre) WHERE"
    if name:
        query += f" n.primaryName CONTAINS \"{name}\" "

    if role:
        #if (role == "DIRECTED" or role == "ACTED_IN"):
        if not name :
            return func.HttpResponse("Parameter 'role' supplied but no 'name' was provided. A 'name' is required to use a filter by 'role'.")
        query += f" AND TYPE(r)=\"{role}\" "

    if genre:
        if name:
            query += " AND "
        query += f" g.genre = \"{genre}\" "
    
    query += " RETURN DISTINCT t.tconst LIMIT 20"
    return query

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    genre = getRequestAttribute(req, 'genre')
    role = getRequestAttribute(req, 'role')
    name = getRequestAttribute(req, 'name')

    server = os.environ["TPBDD_SERVER"]
    database = os.environ["TPBDD_DB"]
    username = os.environ["TPBDD_USERNAME"]
    password = os.environ["TPBDD_PASSWORD"]
    driver= '{ODBC Driver 17 for SQL Server}'

    neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
    neo4j_user = os.environ["TPBDD_NEO4J_USER"]
    neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0 or len(neo4j_server)==0 or len(neo4j_user)==0 or len(neo4j_password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = ""

    if (genre is None and name is None):
        return func.HttpResponse("\tNo suitable parameter was supplied for the request.\n\
        Known parameters are : 'name', 'genre and 'role' (optional, must be combined with 'name'):\n\
        - 'name': name of the artist to filter the request with;\n\
        - 'role': if a name is supplied, restricts the request to the films where the artist had the specified role;\n\
        - 'genre': genre used to filter the request with.\n\
        At least one parameter from the list above is required.\n")
    
    #TODO: Requete pour trouver les films par genre ET OU par nom d'artiste (et avec son rôle si fourni)
    query = buildNeo4jQuery(name, genre, role)
    dataString = query + "\n"
    
    try:
        logging.info("Test de connexion avec py2neo...")
        graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))
        logging.info(query)
        try:
            films_tconst = graph.run(query)
            tconst_list = []
            nb_const_fetched = 0
            query_sql = "SELECT primaryTitle, runtimeMinutes FROM tTitles WHERE tconst= ? "
            for tconst in films_tconst:
                nb_const_fetched += 1
                tconst_list.append(str(tconst).replace("'",""))
                try:
                    logging.info("Test de connexion avec pyodbc...")
                    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
                        cursor = conn.cursor()
                        cursor.execute(query_sql, (str(tconst).replace("'",""),))

                        rows = cursor.fetchall()
                        for row in rows:
                            dataString += f"{name} {role} {genre} : {row[0]} which is {row[1]} minutes long\n"
                except:
                    errorMessage = "Erreur de connexion a la base SQL"
            if nb_const_fetched > 0:
                sql_global_query = "SELECT avg(runtimeMinutes) FROM tTitles WHERE tconst in ( ?"
                for _ in range(len(tconst_list)-1):
                    sql_global_query += ", ?"
                sql_global_query += ")"
                    
                with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
                    cursor = conn.cursor()
                    cursor.execute(sql_global_query, tconst_list)

                    rows = cursor.fetchall()
                    logging.info(rows)
                    for row in rows:
                        logging.info(row)
                        dataString += f"\n\n --> Average duration is {row[0]} minutes\n"
            else:
                dataString += "No data fetched for the given parameters. Please try again with another input and / or refer to the API description.\n"
        except:
            errorMessage = "Erreur de requete"
    except:
        errorMessage = "Erreur de connexion a la base Neo4j"
    
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)
    else:
        return func.HttpResponse(dataString + " Connexions réussies a Neo4j et SQL!")
