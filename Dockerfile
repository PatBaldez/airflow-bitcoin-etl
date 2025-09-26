# Use uma imagem base do Airflow
FROM apache/airflow:2.9.2

# Copie o arquivo requirements.txt para o container
COPY requirements.txt .

# Instale as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copie suas DAGs para a pasta dags do Airflow
COPY dags/ /opt/airflow/dags/
