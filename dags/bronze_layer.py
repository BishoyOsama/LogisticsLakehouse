from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

default_args = {
    'owner': 'bishoy',
}

with DAG(
    'bronze_layer',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:
    
    
    bronze_batch_job = DatabricksRunNowOperator(
        task_id='batch_bronze_task',
        databricks_conn_id='databricks-connection',
        job_id=634435895443898,  
    )

    bronze_stream_job = DatabricksRunNowOperator(
        task_id='streaming_bronze_task',
        databricks_conn_id='databricks-connection',
        job_id=353048162478772,
    )

    [bronze_batch_job, bronze_stream_job]