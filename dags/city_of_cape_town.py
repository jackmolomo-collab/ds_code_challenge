from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def extract():
    print("Starting extraction...")


def transform():
    print("Starting transformation...")


def validate():
    print("Starting validation...")


def write_output():
    print("Writing output...")


with DAG(
    dag_id="city_data_pipeline",
    start_date=datetime(2026, 08, 22),
    schedule=None,
    catchup=False,
    tags=["city", "data-engineering"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=validate,
    )

    write_task = PythonOperator(
        task_id="write_output",
        python_callable=write_output,
    )

    extract_task >> transform_task >> validate_task >> write_task