from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime

# Definir el dataset
mi_dataset = Dataset("my_dataset_s5")

# DAG que produce el dataset
with DAG(
    dag_id='producer_dag_s5',
    start_date=datetime(2025, 6, 3),
    schedule_interval='@daily',
    catchup=False,
) as dag1:
    produce = EmptyOperator(
        task_id='produce_data',
        outlets=[mi_dataset],  # Define que este DAG produce el dataset
    )