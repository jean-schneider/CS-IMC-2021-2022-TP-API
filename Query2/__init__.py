import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import pyodbc as pyodbc
import azure.functions as func
import time


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
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
    nameMessage =""
    description = "Affichage des années de naissance les plus représentées en base de données, avec comparatif des performances des deux bases de données\n\n"    
    
    sql_request = """SELECT TOP 5 *
                    FROM
                        (SELECT birthYear, count(primaryName) nombre
                        FROM tNames 
                        WHERE birthYear != 0 
                        GROUP BY birthYear) decompte
                    ORDER BY nombre DESC"""
    
    neo4j_request = """MATCH (n:Name) 
                        WITH n.birthYear as birthYear, COUNT(n) as nbPersonnes 
                        WHERE birthYear > 0  
                        RETURN birthYear, nbPersonnes 
                        ORDER BY nbPersonnes DESC LIMIT 5"""

    try:
        logging.info("Test de connexion avec py2neo...")
        graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))
        logging.info("Requetage de neo4j...")
        time1 = time.time()
        birthYears = graph.run(neo4j_request)
        time2 = time.time()
        rowsCount = 0
        for birthYear in birthYears:
            dataString += f"- Année {birthYear[0]} avec {birthYear[1]} artistes\n"
            rowsCount += 1
        if rowsCount > 0:
            dataString = "CYPHER:\n" + dataString + f"Elapsed time : {time2-time1} seconds\n\nSQL:\n" 
    except:
        errorMessage = "Erreur de connexion a la base Neo4j"
    try:
        logging.info("Test de connexion avec pyodbc...")
        with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            cursor = conn.cursor()
            time3 = time.time()
            cursor.execute(sql_request)
            rows = cursor.fetchall()
            time4 = time.time()
            if len(rows) > 0:                   
                for row in rows:
                    dataString += f"- Année {row[0]} avec {row[1]} artistes\n"
                dataString += f"Elapsed time : {time4-time3} seconds\n"

    except:
        errorMessage = "Erreur de connexion a la base SQL"
   
    if dataString == "":
        dataString = "Aucune des requêtes n'a abouti. Merci de réessayer ultérieurement.\n"

    if errorMessage != "":
        return func.HttpResponse(description + dataString + nameMessage + errorMessage, status_code=500)

    else:
        return func.HttpResponse(description + dataString + nameMessage + " Connexions réussies a Neo4j et SQL!")
