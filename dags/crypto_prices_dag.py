import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extract import extract_to_raw
from src.transform import transform_raw_to_parquet
from src.load import load_parquet_to_duckdb

DEFAULT_ARGS = {
    "owner": "umi",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="crypto_prices_ingestion",
    default_args=DEFAULT_ARGS,
    description="Ingestão via API (CoinGecko) -> JSON raw -> Parquet -> DuckDB",
    start_date=datetime(2026, 2, 1, tzinfo=timezone.utc),
    schedule="0 * * * *",  # todo início de hora
    catchup=False,
    tags=["portfolio", "api", "etl", "duckdb"],
) as dag:

    def _extract(**context):
        raw_path = extract_to_raw(ids=["bitcoin", "ethereum", "solana"])
        context["ti"].xcom_push(key="raw_path", value=str(raw_path))

    def _transform(**context):
        raw_path = context["ti"].xcom_pull(key="raw_path")
        parquet_path = transform_raw_to_parquet(raw_path)
        context["ti"].xcom_push(key="parquet_path", value=str(parquet_path))

    def _load(**context):
        parquet_path = context["ti"].xcom_pull(key="parquet_path")
        load_parquet_to_duckdb(parquet_path)

    t1 = PythonOperator(task_id="extract_api_to_raw", python_callable=_extract)
    t2 = PythonOperator(task_id="transform_raw_to_parquet", python_callable=_transform)
    t3 = PythonOperator(task_id="load_parquet_to_duckdb", python_callable=_load)

    t1 >> t2 >> t3