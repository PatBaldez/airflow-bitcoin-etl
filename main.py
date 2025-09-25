# Importações necessárias
import requests
import pandas as pd
import pandas_gbq
import os

# --- FUNÇÃO DE EXTRAÇÃO E TRANSFORMAÇÃO ---
def extrair_e_transformar_dados():
    """Extrai e transforma dados históricos do Bitcoin."""
    print("Iniciando a etapa de Extração e Transformação...")
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {'vs_currency': 'brl', 'days': 180}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    precos = data['prices']
    df = pd.DataFrame(precos, columns=['timestamp', 'preco'])
    df['data_hora'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['data'] = df['data_hora'].dt.date
    df = df.astype({'data': 'string', 'preco': 'float64'})
    print("Extração e Transformação concluídas!")
    return df

# --- FUNÇÃO DE CARGA ---
def carregar_dados_no_bigquery(df):
    """Carrega o DataFrame para o BigQuery."""
    print("Iniciando a etapa de Carga...")
    
    # Substitua os valores abaixo pelos seus
    SEU_PROJETO_ID = "patbigcoin"
    SEU_DATASET_ID = "bitcoin_data_pipeline"
    SUA_TABELA_NOME = "cotacoes_bitcoin_patbaldez"
    table_full_id = f'{SEU_DATASET_ID}.{SUA_TABELA_NOME}'
    
    pandas_gbq.to_gbq(
        df,
        destination_table=table_full_id,
        project_id=SEU_PROJETO_ID,
        if_exists='replace',
        chunksize=1000
    )
    print("Dados carregados no BigQuery com sucesso!")

# --- FUNÇÃO PRINCIPAL ---
# Esta função será o "gatilho" que o Cloud Functions irá executar.
def etl_bitcoin(request):
    """
    Função principal que orquestra o ETL.
    """
    df_cotacoes = extrair_e_transformar_dados()
    if df_cotacoes is not None and not df_cotacoes.empty:
        carregar_dados_no_bigquery(df_cotacoes)
    return "Processo ETL concluído!"