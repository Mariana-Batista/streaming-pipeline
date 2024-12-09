import pandas as pd

class DataTransformer():
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = pd.read_csv(file_path)

    def clean_data(self):
        print("Limpando dados...")
        self.data['tempo_assistido'] = self.data['tempo_assistido'].fillna(0)
        self.data['nome_programa'] = self.data['nome_programa'].fillna('Desconhecido')
        self.data['categoria'] = self.data['categoria'].fillna('Desconhecido')
        self.data['dispositivo'] = self.data['dispositivo'].fillna('Desconhecido')
        self.data['data'] = self.data['data'].fillna('1970-01-01')
        self.data['tempo'] = self.data['tempo'].fillna('00:00:00')
        
    def convert_data_type(self):
        print("Convertendo tipos de dados...")
        self.data['tempo_assistido'] = self.data['tempo_assistido'].astype(float)
        self.data['data'] = pd.to_datetime(self.data['data'], errors='coerce')
        self.data['tempo'] = pd.to_timedelta(self.data['tempo'], errors='coerce')
        
        # Convertendo 'tempo' para string no formato HH:MM:SS
        self.data['tempo'] = self.data['tempo'].apply(lambda x: str(x) if pd.notnull(x) else '00:00:00')
        
    def transform(self):
        self.clean_data()
        self.convert_data_type()
        print('Transformando dados...')

        grouped_data = self.data.groupby('categoria')['usuario_id'].nunique().reset_index(name='usuario_count')
        user_count = self.data.groupby('categoria')['nome_programa'].nunique().reset_index(name='programa_count')
        
        """ Fazendo a junção com o dataframe original
        self.data = self.data.merge(grouped_data[['categoria', 'usuario_count']], on='categoria', how='left')
        self.data = self.data.merge(user_count[['categoria', 'programa_count']], on='categoria', how='left')"""
        
        return grouped_data, user_count
    
    def save_transformed_data(self, output_path=None):
        if not output_path:
            output_path = f'../data/processed/transformed_data.csv'
        print(f'Salvando dados transformados em: {output_path}')
        self.data.to_csv(output_path, index=False)
        
file_path = '../data/raw/streaming_data.csv'
transformer = DataTransformer(file_path)

grouped_data, user_count = transformer.transform()
print(grouped_data.head())
print(user_count.head())
"""print(transformer.data.head())  # Agora inclui as novas colunas no dataframe"""

output_path = '../data/processed/transformed_data.csv'
transformer.save_transformed_data(output_path)