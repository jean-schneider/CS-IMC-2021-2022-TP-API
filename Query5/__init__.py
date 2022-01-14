import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import pyodbc as pyodbc
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    genre = req.params.get('genre')
    if not genre:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            genre = req_body.get('genre')
    
    role = req.params.get('role')
    if not role:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            role = req_body.get('role')

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

    query = "MATCH (n:Name)-[r]->(t:Title)"

    name = req.params.get('name')
    if name:
        query += f" WHERE n.primaryName CONTAINS {name}"
    else:
        return func.HttpResponse("Parameter 'name' not found")

    if role:
        #if (role == "DIRECTED" or role == "ACTED_IN"):
        query += f" WHERE TYPE(r)={role}"
    else:
        return func.HttpResponse("Parameter 'role' not found")

    query += " RETURN t.tconst LIMIT 20"

    dataString = query + "\n"
    
    try:
        logging.info("Test de connexion avec py2neo...")
        graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))
        try:
            films_tconst = graph.run(query)
            for tconst in films_tconst:
                #dataString += f"CYPHER: nconst={producer['n.nconst']}, primaryName={producer['n.primaryName']}\n"
                query_sql = f"SELECT primaryTitle, AVG(runtimeMinutes) FROM tTitles WHERE tconst={tconst}"
                try:
                    logging.info("Test de connexion avec pyodbc...")
                    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
                        cursor = conn.cursor()
                        cursor.execute(query)

                        rows = cursor.fetchall()
                        for row in rows:
                            dataString += f"{name} {role} {row[0]} which is {row[1]} minutes long\n"


                except:
                    errorMessage = "Erreur de connexion a la base SQL"
        except:
            errorMessage = "Erreur de requete"
    except:
        errorMessage = "Erreur de connexion a la base Neo4j"
        
    
    
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString + " Connexions réussies a Neo4j et SQL!")
