#!/bin/bash
# Creates one database per service (database-per-service, ARCHITECTURE.md §6)
# on the single local Postgres instance. Runs automatically via Postgres's
# docker-entrypoint-initdb.d hook on first container start.
set -e

for DB in ingestion_db ledger_db settlement_db; do
  echo "Creating database: $DB"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE $DB OWNER $POSTGRES_USER'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$DB')\gexec
EOSQL
done
