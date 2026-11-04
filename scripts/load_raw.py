"""Upload the Olist CSV files to GCS and load them into BigQuery as raw tables."""
import csv
import os
from pathlib import Path

from google.cloud import bigquery, storage

# CSV file name -> raw table name. All columns land as STRING; dbt staging casts them.
TABLES = {
    "olist_customers_dataset.csv": "customers",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_translation",
}


def read_header(path: Path) -> list[str]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return next(csv.reader(f))


def main() -> None:
    project = os.environ["GCP_PROJECT_ID"]
    bucket_name = os.environ["GCS_BUCKET"]
    location = os.environ.get("BQ_LOCATION", "US")
    dataset_id = os.environ.get("BQ_RAW_DATASET", "olist_raw")
    data_dir = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))

    missing = [name for name in TABLES if not (data_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing files in {data_dir}: {', '.join(missing)}")

    bucket = storage.Client(project=project).bucket(bucket_name)
    bq = bigquery.Client(project=project, location=location)

    dataset = bigquery.Dataset(f"{project}.{dataset_id}")
    dataset.location = location
    bq.create_dataset(dataset, exists_ok=True)

    for file_name, table in TABLES.items():
        path = data_dir / file_name
        blob_name = f"raw/olist/{file_name}"
        bucket.blob(blob_name).upload_from_filename(str(path))

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            allow_quoted_newlines=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[bigquery.SchemaField(col, "STRING") for col in read_header(path)],
        )
        job = bq.load_table_from_uri(
            f"gs://{bucket_name}/{blob_name}",
            f"{project}.{dataset_id}.{table}",
            job_config=job_config,
        )
        job.result()
        print(f"Loaded {job.output_rows} rows into {dataset_id}.{table}")


if __name__ == "__main__":
    main()
