import pandas as pd

class DataExtractor():
    def __init__(self, file_path):
        self.file_path = file_path

    def extract_data(self):
        try:
            data = pd.read_csv(self.file_path)
            print(f"Dados extraídos com sucesso de {self.file_path}")
            return data
        except FileNotFoundError:
            print(f"Arquivo {self.file_path} não encontrado.")
            return None
        
if __name__ == "__main__":
    extractor = DataExtractor("../data/processed/streaming_data.csv")
    df = extractor.extract_data()
    print(df.head())
    