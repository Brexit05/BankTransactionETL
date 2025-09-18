from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from batchetl import BatchETL 

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

etl = BatchETL()

def run_etl():
    etl.run()

def close_connection():
    etl.close()

with DAG(
    dag_id='batch_etl_dag',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    run_batch = PythonOperator(
        task_id='run_batch_etl',
        python_callable=run_etl
    )

    close_db = PythonOperator(
        task_id='close_db_connection',
        python_callable=close_connection
    )

    run_batch >> close_db
