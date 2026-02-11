from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'bishoy'
}

with DAG(
    'trigger_dags',
    description='A DAG to trigger other DAGs in defined architectural sequence',
    default_args=default_args,
    schedule=None
) as dag:
     
    trigger_produce_messages = TriggerDagRunOperator(
        task_id='trigger_produce_messages',
        trigger_dag_id='produce_messages',
        wait_for_completion=True,
        poke_interval=30
    ) 


    trigger_bronze_pipeline = TriggerDagRunOperator(
        task_id='trigger_bronze_pipeline',
        trigger_dag_id='bronze_layer',
        wait_for_completion=True,
        poke_interval=30,
    )


    trigger_silver_pipeline = TriggerDagRunOperator(
        task_id='trigger_silver_pipeline',
        trigger_dag_id='silver_layer',
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_dbt_pipeline = TriggerDagRunOperator(
        task_id='trigger_dbt_pipeline',
        trigger_dag_id='dbt_pipeline',
        wait_for_completion=True,
        poke_interval=30
    )



    trigger_produce_messages >> trigger_bronze_pipeline >> trigger_silver_pipeline >> trigger_dbt_pipeline