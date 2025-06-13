from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime

# Definir el dataset
mi_dataset = Dataset("my_dataset_s5")

# DAG que consume el dataset
with DAG(
    dag_id='consumer_dag_s5',
    start_date=datetime(2025, 6, 3),
    schedule=[mi_dataset],  # Define que este DAG se ejecuta cuando el dataset está disponible
    catchup=False,
) as dag2:
    consume = PythonOperator(
        task_id='consume_data',
        python_callable=lambda: print("Dataset consumido"),
    )
