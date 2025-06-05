FROM python:3.11.3-slim-bullseye

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . .

# Instala dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    python3-dev \
    build-essential


RUN pip install -r requirements.txt


EXPOSE 8501