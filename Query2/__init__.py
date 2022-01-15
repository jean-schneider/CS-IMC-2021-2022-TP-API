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

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = "Années de naissances les plus représentées parmi les acteurs :"

    try:
        logging.info("Test de connexion avec pyodbc...")
        with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            cursor = conn.cursor()
            time3 = time.time()
            cursor.execute(f"SELECT TOP 5 *
                FROM
                    (SELECT birthYear, count(primaryName) nombre
                    FROM tNames 
                    WHERE birthYear != 0 
                    GROUP BY birthYear) decompte
                ORDER BY nombre DESC
                ", (filteredName,))
            rows = cursor.fetchall()
            time4 = time.time()
            if len(rows) > 0:                   
                for row in rows:
                    dataString += f"SQL: {row[0]}\n"
                dataString += f"Elapsed time : {time4-time3} seconds\n"

    except:
        errorMessage = "Erreur de connexion a la base SQL"
    
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString + " Connexions réussies a Neo4j et SQL!")
