from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from ingest_prices import ingest_prices

DBT_PROJECT_DIR = "/opt/dbt_crypto"

default_args = {
    "owner": "hamza",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_elt_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
) as dag:

    ingest_prices_task = PythonOperator(
        task_id="ingest_prices_task",
        python_callable=ingest_prices,
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run_task",
        bash_command=f"cd {DBT_PROJECT_DIR} && python -m dbt.cli.main run",
    )

    ingest_prices_task >> dbt_run_task
