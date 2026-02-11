from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta


default_args = {
    'owner': 'bishoy',
}



with DAG(
    'produce_messages',
    default_args=default_args,
    description='A DAG to run the orders producer script',
    schedule=None,
    dagrun_timeout=timedelta(seconds=90)
) as dag:

    run_stream = BashOperator(
        task_id='run_stream',
        bash_command='cd /opt/airflow/scripts/kafka_producer && python main.py --count 20',
        env={
            'BOOTSTRAP_SERVERS': '{{ var.value.bootstrap_servers }}',
            'API_KEY': '{{ var.value.api_key }}',
            'API_SECRET': '{{ var.value.api_secret }}',
            'CLIENT_ID': '{{ var.value.client_id }}'
        }
    )