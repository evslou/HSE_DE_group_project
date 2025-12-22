from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hi():
    print("Airflow is running -- новая версия")

with DAG(
    dag_id="example_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
):
    PythonOperator(
        task_id="hello",
        python_callable=hi
    )