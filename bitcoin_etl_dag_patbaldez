# Mantenha todas as suas importações no topo
import requests
import pandas as pd
import pandera as pa
from datetime import datetime
import pandas_gbq
import os
import functions_framework # Importar para Cloud Functions/Run

# --- Mantenha suas configurações globais (PROJECT_ID, DATASET_ID, TABLE_NAME) aqui ---
# ATENÇÃO: Substitua 'patbigcoin' pelo seu Project ID real do Google Cloud
PROJECT_ID = 'patbigcoin'
DATASET_ID = 'bitcoin_data_patbaldez'
TABLE_NAME = f'cotacoes_bitcoin_patbaldez'

# --- Mantenha seu bitcoin_schema aqui ---
bitcoin_schema = pa.DataFrameSchema(
    columns={
        "data_hora": pa.Column(pa.DateTime, nullable=False),
        "timestamp": pa.Column(pa.Int, nullable=False),
        "preco": pa.Column(float, checks=pa.Check.greater_than(0), nullable=False),
        "data": pa.Column(pa.String, nullable=False),
    },
    unique=["data_hora"],
    strict=True
)

# --- Mantenha suas funções extrair_dados_bitcoin_func, validar_e_transformar_dados_func, carregar_dados_no_bigquery_func aqui ---
def extrair_dados_bitcoin_func(moeda='usd', dias=180):
    print(f"Extraindo dados do Bitcoin para os últimos {dias} dias (moeda: {moeda})...")
    url = f"https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {
        'vs_currency': moeda,
        'days': dias,
        'interval': 'daily'
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    precos = data['prices']
    df = pd.DataFrame(precos, columns=['timestamp', 'preco'])
    df['data_hora'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['data'] = df['data_hora'].dt.date.astype(str)
    df = df[['data_hora', 'timestamp', 'preco', 'data']]
    print("Extração concluída com sucesso!")
    return df.to_json(date_format='iso', orient='records')

def validar_e_transformar_dados_func(df_json):
    print("Validando e transformando dados...")
    df = pd.read_json(df_json)
    try:
        validated_df = bitcoin_schema.validate(df)
        print("Dados validados com Pandera com sucesso!")
    except pa.errors.SchemaErrors as err:
        print("Erros de validação de esquema encontrados:")
        print(err.failure_cases)
        raise
    validated_df.set_index('data_hora', inplace=True)
    print("Transformação concluída com sucesso!")
    return validated_df.to_json(date_format='iso', orient='records')

def carregar_dados_no_bigquery_func(df_json, project_id, dataset_id, table_name):
    print(f"Carregando dados para BigQuery no projeto {project_id}, dataset {dataset_id}, tabela {table_name}...")
    df = pd.read_json(df_json)
    if df.empty:
        print("DataFrame está vazio. Nenhuma carga será realizada.")
        return
    df['preco'] = df['preco'].astype(float)
    table_id = f'{dataset_id}.{table_name}'
    pandas_gbq.to_gbq(
        df,
        destination_table=table_id,
        project_id=project_id,
        if_exists='replace',
        progress_bar=False
    )
    print("Dados carregados no BigQuery com sucesso!")

# --- FUNÇÃO PRINCIPAL PARA O CLOUD RUN ---
@functions_framework.http
def run_bitcoin_etl(request):
    print("Iniciando o pipeline ETL do Bitcoin no Cloud Run...")
    try:
        extracted_data_json = extrair_dados_bitcoin_func(moeda='usd', dias=180)
        transformed_data_json = validar_e_transformar_dados_func(extracted_data_json)
        carregar_dados_no_bigquery_func(transformed_data_json, PROJECT_ID, DATASET_ID, TABLE_NAME)
        print("Pipeline ETL do Bitcoin concluído com sucesso!")
        return 'ETL do Bitcoin executado com sucesso!', 200
    except Exception as e:
        print(f"Erro durante a execução do ETL: {e}")
        return f'Erro durante o ETL: {e}', 500
