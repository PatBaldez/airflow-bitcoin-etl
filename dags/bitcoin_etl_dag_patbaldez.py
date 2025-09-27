# dags/bitcoin_etl_dag_patbaldez.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import pandera as pa
import pandas_gbq

default_args = {
    'owner': 'patbaldez',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

PROJECT_ID = 'patbigcoin'
DATASET_ID = 'bitcoin_data_patbaldez'
TABLE_NAME = 'cotacoes_bitcoin_patbaldez'

def extract_bitcoin_data():
    """Extrai dados do Bitcoin da CoinGecko API"""
    print("Extraindo dados do Bitcoin...")
    url = 'https://api.coingecko.com/api/v3/coins/bitcoin/market_chart'
    params = {
        'vs_currency': 'usd',
        'days': 180,
        'interval': 'daily'
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    prices = data['prices']
    
    df = pd.DataFrame(prices, columns=['timestamp', 'price'])
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['date_str'] = df['date'].dt.strftime('%Y-%m-%d')
    
    print(f"Extraídos {len(df)} registros")
    return df.to_dict('records')

def validate_data(**context):
    """Valida os dados com Pandera"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    df = pd.DataFrame(data)
    
    # Schema de validação
    schema = pa.DataFrameSchema({
        "timestamp": pa.Column(pa.Int64, nullable=False),
        "price": pa.Column(pa.Float, checks=pa.Check.greater_than(0)),
        "date": pa.Column(pa.DateTime, nullable=False),
        "date_str": pa.Column(pa.String, nullable=False)
    })
    
    validated_df = schema.validate(df)
    print("Dados validados com sucesso!")
    return validated_df.to_dict('records')

def load_to_bigquery(**context):
    """Carrega dados para o BigQuery"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='validate_data')
    df = pd.DataFrame(data)
    
    # Prepara os dados para BigQuery
    df_bq = df[['date_str', 'price', 'timestamp']].copy()
    df_bq.columns = ['data', 'preco', 'timestamp']
    
    table_id = f"{DATASET_ID}.{TABLE_NAME}"
    
    pandas_gbq.to_gbq(
        df_bq,
        table_id,
        project_id=PROJECT_ID,
        if_exists='replace'
    )
    print(f"Dados carregados para {table_id}")

# Definição da DAG
with DAG(
    'bitcoin_etl_patbaldez',
    default_args=default_args,
    description='ETL para dados do Bitcoin - PatBaldez',
    schedule_interval='@daily',
    catchup=False,
    tags=['bitcoin', 'etl', 'patbaldez']
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_bitcoin_data
    )
    
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )
    
    load_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery
    )
    
    # Ordem de execução
    extract_task >> validate_task >> load_task
