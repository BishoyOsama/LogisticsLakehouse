from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

default_args = {
    'owner': 'bishoy'
}

with DAG(
    'silver_layer',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:
    
    silver_batch_job = DatabricksRunNowOperator(
        task_id='batch_silver_task',
        databricks_conn_id='databricks-connection',
        job_id=809977803520849,  
    )

    silver_streaming_job = DatabricksRunNowOperator(
        task_id='streaming_silver_task',
        databricks_conn_id='databricks-connection',
        job_id=762887421683058,
    )    
    

    """ bronze_batch_job = DatabricksRunNowOperator(
        task_id='batch_bronze_task',
        databricks_conn_id='databricks-connection',
        job_id=634435895443898,  
    ) """

    [silver_batch_job, silver_streaming_job]