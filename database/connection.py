import mysql.connector
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv() # Carrega as variáveis de ambiente do arquivo .env

class DataLoader():
    def __init__(self):
        self.host = os.getenv('MYSQL_HOST')
        self.user = os.getenv('MYSQL_USER')
        self.password = os.getenv('MYSQL_PASSWORD')
        self.database = os.getenv('MYSQL_DATABASE')
        self.connection = None
        
    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            print("Conexão estabelecida.")
            
        except mysql.connector.Error as error:
            print(f"Falha na conexão: {error}")
    
    def load(self, data: pd.DataFrame, table_name: str):
        if self.connection is None:
            print("Conexão não estabelecida.")
            return
        
        cursor  = self.connection.cursor()
        try:
            for _, row in data.iterrows():
                cursor.execute(f"""
                               INSERT INTO {table_name} (usuario_id, nome_programa, categoria, tempo_assistido, dispositivo, data, tempo) 
                               VALUES (%s, %s, %s, %s, %s, %s, %s)
                               """, 
                               tuple(row))
                self.connection.commit()
                print("Dados carregados com sucesso!")
        except mysql.connection.Error as error:
            print(f"Falha ao inserir dados: {error}")         
        finally:        
            cursor.close() 
            
    def close(self):
        if self.connection is not None:
            self.connection.close()
            print("Conexão encerrada.") #Fechando a conexão

if __name__ == "__main__":
    TABLE_NAME = "streaming_data" #Nome da tabela onde os dados serão inseridos
    
    data = pd.read_csv("../data/raw/streaming_data.csv")
    
    loader = DataLoader()
    loader.connect()
    loader.load(data, TABLE_NAME)
    loader.close()
