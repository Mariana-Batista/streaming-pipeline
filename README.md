# Streaming Data Pipeline

<p>Este projeto realiza a transformação e o carregamento de dados de streaming para um banco de dados MySQL. O pipeline processa os dados brutos, aplicando limpeza, conversão de tipos e análise, e carrega os dados transformados na base de dados. 

## Funcionalidades
    1. Transformação de dados: Limpeza, conversão de tipos de dados e agrupamento por categoria.
    2. Carregamento de dados: Inserção dos dados transformados em uma tabela no MySQL.
    3. Configuração de banco de dados: Criação da tabela, caso ela não exista.
    4. Criação de dashboard: Uso do Streamlit para criar um dashboard.

## Tecnologias Utilizadas
    . Python
    . pandas
    . MySQL
    . Streamlit
    . dotenv (para variáveis de ambiente)

## Estrutura do projeto
    
    streaming-pipeline/
    │
    ├── dashboard/
    │   ├── app.py             # Script do streamlit
    │   └── helpers.py         # Script de conexão com o banco de dados
    |   └── queries.py         # Script de consulta ao banco de dados
    │
    ├── data/
    │   ├── raw/               # Dados brutos de entrada
    │   └── processed/         # Dados processados e transformados
    │
    ├── scripts/
    |   ├── extract.py              
    │   ├── transform.py       # Script de transformação de dados
    │   └── load.py            # Script para carregar os dados no MySQL
    │
    ├── .gitignore
    ├── .env                   # Arquivo de variáveis de ambiente
    ├── requirements.txt       # Arquivo de dependências
    └── README.md              # Este arquivo

## Contribuições
<p> Contribuições são bem-vindas! Se você tiver alguma sugestão ou correção, fique à vontade para abrir uma issue ou enviar um pull request.

## Licença
<p> Este projeto é licenciado sob a licença MIT.
