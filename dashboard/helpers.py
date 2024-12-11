from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def fetch_data_from_db(query):
    """Executa a consulta SQL e retorna os resultados como um DataFrame."""
    db_url = f"mysql+pymysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@{os.getenv('MYSQL_HOST')}/{os.getenv('MYSQL_DATABASE')}"
    engine = create_engine(db_url)
    
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"Erro ao consultar o banco de dados: {e}")
        return pd.DataFrame()