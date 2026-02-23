Crypto Prices Ingestion (API → Parquet → DuckDB) with Airflow

Visão geral

Pipeline agendado (Airflow) que coleta preços via API (CoinGecko), salva raw JSON, transforma em Parquet e carrega em DuckDB.

Arquitetura

API → data/raw/ → data/processed/ → warehouse/warehouse.duckdb

Orquestração: Airflow (Docker Compose)

Camadas: raw / processed / warehouse

Stack

Python, Airflow, DuckDB, Docker, Parquet

Como rodar local

Pré-requisitos (Docker Desktop)

docker compose build

docker compose up -d

UI: http://localhost:8080

Login (se você definiu)

DAG

Nome da DAG: crypto_prices_ingestion

Schedule: 0 * * * * (exemplo)

Tasks: extract → transform → load

Validação

SQL no DuckDB (exemplos de query)

Prints do Airflow graph + sucesso

Conectar no Power BI

Opção ODBC (DSN + query)

Opção Parquet (nativo)

Melhorias futuras

retries/backoff, tests, particionamento por data, alertas, dbt, etc.
