# dashboard/app.py
import streamlit as st
import pandas as pd
from helpers import fetch_data_from_db
from queries import get_categoria_count_query

# Título da aplicação
st.title('Dashboard de Análise de Dados')

# Carregar e exibir dados de categoria
st.header('Contagem de usuários por categoria')
query = get_categoria_count_query()
data = fetch_data_from_db(query)

print(data)

st.write(data)

# Gráfico de barras (Exemplo)
st.bar_chart(data.set_index('categoria')['usuario_count'])
