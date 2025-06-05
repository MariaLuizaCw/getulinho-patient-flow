FROM apache/airflow:2.8.1-python3.10

USER root

# Instala dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    python3-dev \
    build-essential



RUN date
ENV TZ=America/Sao_Paulo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN date

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

