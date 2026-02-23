from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

def load_parquet_to_duckdb(parquet_path: str | Path, db_path: str = "/opt/airflow/warehouse/warehouse.duckdb") -> None:
    parquet_path = str(parquet_path)
    db_path = str(db_path)

    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_crypto_prices (
            coin_id VARCHAR,
            price_usd DOUBLE,
            last_updated_at_unix BIGINT,
            extracted_at_utc VARCHAR
        );
    """)

    df = pd.read_parquet(parquet_path)

    # Evita duplicar: remove linhas com extracted_at_utc já existente
    existing = con.execute(
        "SELECT DISTINCT extracted_at_utc FROM fact_crypto_prices WHERE extracted_at_utc = ?",
        [df["extracted_at_utc"].iloc[0]]
    ).fetchall()

    if existing:
        # já carregou esse lote
        con.close()
        return

    con.register("df_view", df)
    con.execute("INSERT INTO fact_crypto_prices SELECT * FROM df_view;")
    con.close()

if __name__ == "__main__":
    import sys
    load_parquet_to_duckdb(sys.argv[1])
    print("Loaded into DuckDB")