from airflow.decorators import dag
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='bkk_bronze_processing_hourly',
    default_args=default_args,
    description='Triggers PySpark batch processing for Bronze Layer',
    schedule='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bkk', 'bronze', 'pyspark'],
)
def bkk_bronze_pipeline():
    run_pyspark_job = BashOperator(
        task_id='run_pyspark_bronze',
        bash_command="python /opt/airflow/workspace/bronze_batch.py"
    )
    run_pyspark_job


bkk_bronze_pipeline()
