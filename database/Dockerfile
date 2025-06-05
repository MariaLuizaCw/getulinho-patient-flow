FROM postgres:16

# Instala dependências e o pg_partman
RUN apt-get update && \
    apt-get install -y postgresql-16-partman postgresql-contrib && \
    rm -rf /var/lib/apt/lists/*

# Copia scripts de inicialização
COPY ./init/ /docker-entrypoint-initdb.d/
