from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello():
    print("Hello from Airflow !")


default_args = {
    "owner": "hamza",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="hello_world_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # pas de planification automatique
    catchup=False,
    tags=["test"],
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )
