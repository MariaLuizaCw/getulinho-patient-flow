#!/bin/bash

# ID ou nome do container
CONTAINER_NAME="4a326f27f315"

# Caminho correto dentro do container
DUMP_FILE=/var/lib/postgresql/data/backup.dump

# PostgreSQL
POSTGRES_USER=getulinho
POSTGRES_PASSWORD=int1234
POSTGRES_DB=postgres


# Executa o restore
docker exec -e PGPASSWORD=$POSTGRES_PASSWORD $CONTAINER_NAME \
  pg_restore -U $POSTGRES_USER -d $POSTGRES_DB $DUMP_FILE


