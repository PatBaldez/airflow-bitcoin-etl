# Importações necessárias para Airflow e outras bibliotecas
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator # Para iniciar e finalizar o pipeline
import pendulum
import requests
import pandas as pd
import pandera as pa
from datetime import datetime
import pandas_gbq
import os

# --- Configurações globais ---
# ATENÇÃO: Substitua 'patbigcoin' pelo seu Project ID real do Google Cloud
PROJECT_ID = 'patbigcoin'
# ATENÇÃO: Substitua 'seu_dataset' pelo ID do seu BigQuery Dataset
# Crie este dataset no BigQuery se ele ainda não existir!
DATASET_ID = 'bitcoin_data_patbaldez'
# Nome da tabela no BigQuery com seu sufixo
TABLE_NAME = f'cotacoes_bitcoin_patbaldez' # Adicionando o sufixo conforme o requisito

# --- 1. Definição do Schema com Pandera ---
# Vamos definir um esquema para validar os dados extraídos do Bitcoin.
# Isso garante que as colunas 'data_hora', 'data' e 'preco' estejam no formato esperado.
# A validação será um passo importante da transformação (T do ETL).
bitcoin_schema = pa.DataFrameSchema(
    columns={
        "data_hora": pa.Column(pa.DateTime, nullable=False),
        "timestamp": pa.Column(pa.Int, nullable=False), # O timestamp original
        "preco": pa.Column(float, checks=pa.Check.greater_than(0), nullable=False),
        "data": pa.Column(pa.String, nullable=False), # A data em formato de string, para consistência com o BigQuery
    },
    # Garante que não haja duplicatas pela data_hora
    unique=["data_hora"],
    # Opcional: Garante que o index seja de data e hora
    # index=pa.Index(pa.DateTime),
    strict=True # Garante que não haja colunas extras
)


# --- Funções Auxiliares (fora da DAG para reuso e limpeza) ---

def extrair_dados_bitcoin_func(moeda='usd', dias=180):
    """
    Extrai dados históricos de preços do Bitcoin da API da CoinGecko.
    Esta função será encapsulada na task 'extract'.
    """
    print(f"Extraindo dados do Bitcoin para os últimos {dias} dias (moeda: {moeda})...")
    url = f"https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {
        'vs_currency': moeda,
        'days': dias,
        'interval': 'daily' # Garantimos que é diário como no seu código original
    }
    response = requests.get(url, params=params)
    response.raise_for_status() # Lança um erro para requisições com falha
    data = response.json()
    precos = data['prices']
    df = pd.DataFrame(precos, columns=['timestamp', 'preco'])
    df['data_hora'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['data'] = df['data_hora'].dt.date.astype(str) # Converte para string para o BigQuery
    
    # Reordenar as colunas para o esquema
    df = df[['data_hora', 'timestamp', 'preco', 'data']]
    
    print("Extração concluída com sucesso!")
    # Retornamos o DataFrame em um formato serializável (por exemplo, JSON ou CSV temporário)
    # Airflow 2.x com TaskFlow API pode lidar com DataFrames diretamente, mas para ser seguro
    # e compatível com XCom, podemos serializar ou salvar em um arquivo temporário.
    # Por simplicidade, para Airflow Tasks, retornaremos o df serializado como JSON.
    return df.to_json(date_format='iso', orient='records')

def validar_e_transformar_dados_func(df_json):
    """
    Valida e transforma os dados do Bitcoin.
    Esta função será encapsulada na task 'transform'.
    """
    print("Validando e transformando dados...")
    df = pd.read_json(df_json)
    
    # Validação com Pandera
    try:
        validated_df = bitcoin_schema.validate(df)
        print("Dados validados com Pandera com sucesso!")
    except pa.errors.SchemaErrors as err:
        print("Erros de validação de esquema encontrados:")
        print(err.failure_cases)
        raise  # Levanta o erro para que a DAG falhe

    # Transformação adicional (ex: definir o índice)
    validated_df.set_index('data_hora', inplace=True)
    
    print("Transformação concluída com sucesso!")
    return validated_df.to_json(date_format='iso', orient='records')

def carregar_dados_no_bigquery_func(df_json, project_id, dataset_id, table_name):
    """
    Carrega um DataFrame do pandas para uma tabela no Google BigQuery.
    Esta função será encapsulada na task 'load'.
    """
    print(f"Carregando dados para BigQuery no projeto {project_id}, dataset {dataset_id}, tabela {table_name}...")
    df = pd.read_json(df_json)

    if df.empty:
        print("DataFrame está vazio. Nenhuma carga será realizada.")
        return

    # Converte os tipos de dados para evitar erros de schema no BigQuery, se necessário.
    # pandas-gbq geralmente faz um bom trabalho de inferência, mas podemos forçar:
    df['preco'] = df['preco'].astype(float)
    # A coluna 'data' já é string e 'data_hora' será TIMESTAMP no BQ.

    table_id = f'{dataset_id}.{table_name}'

    pandas_gbq.to_gbq(
        df,
        destination_table=table_id,
        project_id=project_id,
        if_exists='replace', # 'replace' substitui a tabela a cada execução da DAG
        progress_bar=False # Desativa a barra de progresso em ambientes de Airflow
    )
    print("Dados carregados no BigQuery com sucesso!")


# --- Definição da DAG ---

@dag(
    dag_id='bitcoin_etl_patbaldez', # ID único da DAG
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), # Data de início
    schedule_interval='@daily', # Executa diariamente
    catchup=False, # Não executa para datas passadas
    tags=['bitcoin', 'etl', 'bigquery', 'patbaldez'], # Tags para organização
)
def bitcoin_etl_workflow():
    start = EmptyOperator(task_id='start_etl')

    # Task de Extração
    @task
    def extract_bitcoin_data():
        return extrair_dados_bitcoin_func(moeda='usd', dias=180) # Alterado para USD como no seu primeiro exemplo de DAG

    # Task de Transformação (inclui validação Pandera)
    @task
    def transform_and_validate_bitcoin_data(df_json):
        return validar_e_transformar_dados_func(df_json)

    # Task de Carga
    @task
    def load_bitcoin_data(df_json):
        carregar_dados_no_bigquery_func(df_json, PROJECT_ID, DATASET_ID, TABLE_NAME)

    end = EmptyOperator(task_id='end_etl')

    # --- Definição da Ordem das Tasks (Pipeline) ---
    start >> extract_bitcoin_data() >> transform_and_validate_bitcoin_data(extract_bitcoin_data().output) >> load_bitcoin_data(transform_and_validate_bitcoin_data(extract_bitcoin_data().output).output) >> end

# Instancia a DAG para que o Airflow a detecte
bitcoin_etl_dag = bitcoin_etl_workflow()
