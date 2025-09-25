# Importa��es necess�rias do Airflow e outras bibliotecas
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# As fun��es de extra��o e carregamento que criamos. 
# Podemos coloc�-las no mesmo arquivo ou import�-las de outro.
import requests
import pandas as pd
import pandas_gbq
import os

# --- FUN��ES DE EXTRA��O E CARGA ---
@task
def extrair_e_transformar_dados(moeda: str, dias: int):
    """Extrai e transforma dados hist�ricos do Bitcoin."""
    print("Iniciando a etapa de Extra��o e Transforma��o...")
    url = f"https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {'vs_currency': moeda, 'days': dias}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    precos = data['prices']
    df = pd.DataFrame(precos, columns=['timestamp', 'preco'])
    df['data_hora'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['data'] = df['data_hora'].dt.date
    # A transforma��o aqui � simples: converter tipos e formatar
    df = df.astype({'data': 'string', 'preco': 'float64'})
    print("Extra��o e Transforma��o conclu�das!")
    return df

@task
def carregar_dados_no_bigquery(df: pd.DataFrame, project_id: str, table_id: str):
    """Carrega o DataFrame para o BigQuery."""
    print("Iniciando a etapa de Carga...")
    pandas_gbq.to_gbq(
        df,
        destination_table=table_id,
        project_id=project_id,
        if_exists='replace',
        chunksize=1000 # Otimiza a carga de grandes datasets
    )
    print("Dados carregados no BigQuery com sucesso!")


# --- DEFINI��O DA DAG ---
@dag(
    dag_id="bitcoin_etl_dag_SEU_NOME", # <- Mude para o seu nome
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['bitcoin', 'etl', 'bigquery'],
)
def bitcoin_etl_pipeline():
    """
    DAG de ETL que extrai dados de cota��o do Bitcoin e os carrega para o BigQuery.
    """
    # Substitua os valores abaixo pelos seus
    SEU_PROJETO_ID = "patbigcoin"
    SEU_DATASET_ID = "bitcoin_data_pipeline"
    SUA_TABELA_NOME = "cotacoes_bitcoin_SEU_NOME"
    
    # Nome completo da tabela no formato 'dataset.tabela'
    table_full_id = f'{SEU_DATASET_ID}.{SUA_TABELA_NOME}'
    
    # 1. Tarefa de Extra��o e Transforma��o
    df_cotacoes = extrair_e_transformar_dados(moeda='brl', dias=180)
    
    # 2. Tarefa de Carregamento
    carregar_dados_no_bigquery(df_cotacoes, SEU_PROJETO_ID, table_full_id)


# Instanciando a DAG para que o Airflow a reconhe�a
bitcoin_etl_pipeline()
