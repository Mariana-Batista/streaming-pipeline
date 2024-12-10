# dashboard/helpers.py
import mysql.connector
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DATABASE'),
        )
        print("Conexão com o banco de dados bem-sucedida!")
        return connection
    except mysql.connector.Error as err:
        print(f"Erro de conexão: {err}")
        return None

def fetch_data_from_db(query):
    db_connection = get_db_connection()
    data = pd.read_sql(query, db_connection)
    db_connection.close()
    return data
