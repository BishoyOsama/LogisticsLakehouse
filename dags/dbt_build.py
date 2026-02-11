from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'bishoy',
}

with DAG(
    'dbt_pipeline',
    description='A DAG to run the dbt pipeline',
    default_args=default_args,
    schedule=None
) as dag:
    
    run_dbt_model = BashOperator(
        task_id='build_dbt',
        bash_command='cd /opt/airflow/databricks_dbt && dbt clean && dbt build'
    )