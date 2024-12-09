import pandas as pd
import mysql.connector
from transform import DataTransformer
import os
from dotenv import load_dotenv

load_dotenv() # Carrega as variáveis de ambiente do arquivo .env

class DataTransformLoader():
    def __init__(self, db_connection, table_name="streaming_data_transform"):
        self.db_connection = db_connection
        self.table_name = table_name
    
    def create_table(self):
        cursor = self.db_connection.cursor()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            usuario_id INT,
            nome_programa VARCHAR(255),
            categoria VARCHAR(255),
            tempo_assistido FLOAT,
            dispositivo VARCHAR(255),
            data DATETIME,
            tempo TIME,
            usuario_count INT,
            programa_count INT
        );
        """
        cursor.execute(create_table_query)
        self.db_connection.commit()
        print(f'Tabela {self.table_name} criada ou já existente')
        
    def insert_data(self, data):
        cursor = self.db_connection.cursor()
        insert_query = f"""
        INSERT INTO {self.table_name} (usuario_id, nome_programa, categoria, tempo_assistido, dispositivo, data, tempo, usuario_count, programa_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        for _, row in data.iterrows():  # 'data' contém os dados processados
            cursor.execute(insert_query, tuple(row))
        self.db_connection.commit()
        print(f'Dados carregados com sucesso na tabela {self.table_name}.')

    def load_data(self, file_path):
        # Instanciando o DataTransformer e transformando os dados
        transformer = DataTransformer(file_path)
        grouped_data, user_count = transformer.transform()  # Transformando os dados
        self.create_table()  # Criar a tabela no banco de dados (se necessário)
        self.insert_data(transformer.data)  # Inserir dados transformados
        

# Exemplo de uso:
if __name__ == "__main__":
    # Estabelecendo a conexão com o banco de dados
    db_connection = mysql.connector.connect(
        host = os.getenv('MYSQL_HOST'),
        user = os.getenv('MYSQL_USER'),
        password = os.getenv('MYSQL_PASSWORD'),
        database = os.getenv('MYSQL_DATABASE'),
    )
    
    # Caminho do arquivo de dados transformados
    file_path = '../data/processed/transformed_data.csv'

    # Criar uma instância de DataLoader e carregar os dados
    loader = DataTransformLoader(db_connection)
    loader.load_data(file_path)
    
    # Fechar a conexão com o banco de dados
    db_connection.close()
    
    ### verificar o erro, o número de colunas está incorreto!"
    