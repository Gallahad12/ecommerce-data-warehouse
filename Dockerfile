FROM apache/airflow:2.10.5-python3.11

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt

# dbt runs from its own virtualenv so its dependencies cannot conflict with Airflow.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core~=1.12.0" "dbt-bigquery~=1.12.0"
