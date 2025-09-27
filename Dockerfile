FROM apache/airflow:2.7.0

# Alternativa se a versão 2.7.0 não estiver disponível:
# FROM apache/airflow:2.8.1

USER root

# Instala dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copia o requirements.txt primeiro (para cache do Docker)
COPY requirements.txt .

# Instala dependências do Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia a pasta dags para o container
COPY dags/ /opt/airflow/dags/
