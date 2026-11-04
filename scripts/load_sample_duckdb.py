"""Load the small sample CSVs into DuckDB so CI can run dbt without GCP credentials."""
import os
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DIR = ROOT / "dbt" / "sample_data"
DB_PATH = ROOT / "dbt" / "ci.duckdb"
RAW_SCHEMA = os.environ.get("BQ_RAW_DATASET", "olist_raw")


def main() -> None:
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"create schema if not exists {RAW_SCHEMA}")
    for path in sorted(SAMPLE_DIR.glob("*.csv")):
        con.execute(
            f"create or replace table {RAW_SCHEMA}.{path.stem} as "
            "select * from read_csv(?, header = true, all_varchar = true)",
            [str(path)],
        )
        print(f"Loaded {RAW_SCHEMA}.{path.stem}")
    con.close()


if __name__ == "__main__":
    main()
