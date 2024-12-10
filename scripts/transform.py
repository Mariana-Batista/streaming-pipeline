import pandas as pd

class DataTransformer:
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
        self.data['tempo'] = self.data['tempo'].apply(
            lambda x: str(x).split()[-1] if pd.notnull(x) else '00:00:00'
        )


    def transform(self):
        print("Transformando dados...")
        self.clean_data()
        self.convert_data_type()

    def save_transformed_data(self, output_path='../data/processed/transformed_data.csv'):
        print(f"Salvando dados transformados em: {output_path}")
        self.data.to_csv(output_path, index=False)


# Uso do script
file_path = '../data/raw/streaming_data.csv'
transformer = DataTransformer(file_path)
transformer.transform()
transformer.save_transformed_data()