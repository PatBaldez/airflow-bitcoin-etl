# Use uma imagem Python oficial como base
FROM python:3.9-slim-buster

# Define o diretório de trabalho no contêiner
WORKDIR /app

# Copia o arquivo requirements.txt para o contêiner
COPY requirements.txt .

# Instala as dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia seu script ETL (assumindo que ele está na raiz ou na pasta 'dags' e renomeamos para a raiz)
# Se seu arquivo ainda está em dags/bitcoin_etl_dag_patbaldez.py,
# COLOQUE ele na RAIZ do seu repositório GitHub para esta abordagem simplificada,
# ou ajuste a linha COPY abaixo para:
# COPY dags/bitcoin_etl_dag_patbaldez.py /app/main.py
COPY bitcoin_etl_dag_patbaldez.py /app/main.py

# A instrução ENTRYPOINT e CMD são usadas para configurar o Cloud Run
# O Cloud Run espera que um servidor HTTP seja iniciado.
# 'functions-framework' vai iniciar um servidor HTTP que executa sua função 'run_bitcoin_etl'
ENV PORT 8080
CMD exec functions-framework --target=run_bitcoin_etl --port=$PORT
