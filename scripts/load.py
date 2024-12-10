import pandas as pd
import mysql.connector
import os
from dotenv import load_dotenv
from transform import DataTransformer
from transform_group import DataTransformGroup

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

class DataLoader:
    def __init__(self, db_connection, table_name):
        self.db_connection = db_connection
        self.table_name = table_name

    def create_table(self, table_schema):
        """Cria a tabela no banco de dados, se ela não existir."""
        cursor = self.db_connection.cursor()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            {table_schema}
        );
        """
        cursor.execute(create_table_query)
        self.db_connection.commit()
        print(f"Tabela {self.table_name} criada ou já existente.")

    def insert_data(self, data):
        """Insere dados na tabela."""
        cursor = self.db_connection.cursor()
        columns = ', '.join(data.columns)
        placeholders = ', '.join(['%s'] * len(data.columns))
        insert_query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});"

        for _, row in data.iterrows():
            cursor.execute(insert_query, tuple(row))
        self.db_connection.commit()
        print(f"Dados carregados com sucesso na tabela {self.table_name}.")

    def load_data(self, file_path, table_schema):
        """Carrega os dados do arquivo CSV para a tabela."""
        # Carregar os dados do CSV
        data = pd.read_csv(file_path)
        # Criar a tabela com base no schema fornecido
        self.create_table(table_schema)
        # Inserir os dados na tabela
        self.insert_data(data)

if __name__ == "__main__":
    # Estabelecer conexão com o banco de dados
    db_connection = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DATABASE'),
    )

    try:
        # Tabela 1: Dados transformados
        transformed_file_path = '../data/processed/transformed_data.csv'
        transformed_table_name = 'transformed_data'
        transformed_schema = """
            usuario_id INT,
            nome_programa VARCHAR(255),
            categoria VARCHAR(255),
            tempo_assistido FLOAT,
            dispositivo VARCHAR(255),
            data DATETIME,
            tempo TIME
        """
        transformed_loader = DataLoader(db_connection, transformed_table_name)
        transformed_loader.load_data(transformed_file_path, transformed_schema)

        """# Tabela 2: Dados de análise
        analysis_file_path = '../data/processed/data_analysis.csv'
        analysis_table_name = 'data_analysis'
        analysis_schema = """
            #categoria VARCHAR(255),
            #usuario_count INT,
            #programa_count INT
        """
        analysis_loader = DataLoader(db_connection, analysis_table_name)
        analysis_loader.load_data(analysis_file_path, analysis_schema)"""

    finally:
        # Fechar a conexão com o banco de dados
        db_connection.close()
