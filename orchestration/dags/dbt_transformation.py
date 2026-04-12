from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='bkk_dbt_transformations_hourly',
    default_args=default_args,
    description='Triggers dbt jobs for Silver and Gold Databricks transformations',
    schedule='@hourly',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['bkk', 'silver', 'gold', 'dbt'],
)
def bkk_dbt_pipeline():
    run_dbt_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command='cd /opt/airflow/dbt_transform && dbt run --select models/silver'
    )

    run_dbt_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command='cd /opt/airflow/dbt_transform && dbt run --select models/gold'
    )

    run_dbt_test = BashOperator(
        task_id='dbt_test_models',
        bash_command='cd /opt/airflow/dbt_transform && dbt test'
    )

    run_dbt_silver >> run_dbt_gold >> run_dbt_test

bkk_dbt_pipeline()