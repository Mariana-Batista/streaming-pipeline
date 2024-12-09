# Streaming Data Pipeline

<p>Este projeto realiza a transformação e o carregamento de dados de streaming para um banco de dados MySQL. O pipeline processa os dados brutos, aplicando limpeza, conversão de tipos e análise, e carrega os dados transformados na base de dados.

## Funcionalidades
    1. Transformação de dados: Limpeza, conversão de tipos de dados e agrupamento por categoria.
    2. Carregamento de dados: Inserção dos dados transformados em uma tabela no MySQL.
    3. Configuração de banco de dados: Criação da tabela, caso ela não exista.

## Tecnologias Utilizadas
    . Python
    . pandas
    . MySQL
    . dotenv (para variáveis de ambiente)

## Estrutura do projeto
    
    streaming-pipeline/
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
