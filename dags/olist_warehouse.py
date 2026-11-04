"""Load Olist data into BigQuery, then build and test the warehouse with dbt."""
import json
import os
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT = "/home/airflow/dbt-venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt"


def alert_on_failure(context) -> None:
    """Post to ALERT_WEBHOOK_URL when a task fails. Does nothing if it is not set."""
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if not url:
        return
    ti = context["task_instance"]
    text = f":red_circle: {ti.dag_id}.{ti.task_id} failed. Logs: {ti.log_url}"
    request = urllib.request.Request(
        url,
        data=json.dumps({"text": text}).encode(),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(request, timeout=10)


with DAG(
    dag_id="olist_warehouse",
    description="GCS and BigQuery load followed by dbt build",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alert_on_failure,
    },
    tags=["olist", "dbt", "bigquery"],
) as dag:
    load_raw = BashOperator(
        task_id="load_raw",
        bash_command="python /opt/airflow/scripts/load_raw.py",
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && {DBT} deps",
    )

    # dbt build runs each model followed by its tests, so bad data stops downstream models.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {DBT} build --target dev",
    )

    load_raw >> dbt_deps >> dbt_build
