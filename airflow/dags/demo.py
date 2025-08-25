
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

SPARK_URL= os.getenv("SPARK_URL")

with DAG('demo',
        schedule_interval=None,
        start_date = days_ago(1),
        catchup=False,
        tags=['batch']
) as dag:

    seed_kafka = BashOperator(
        task_id='seed_kafka',
        bash_command='python /opt/project/scripts/demo/seed_kafka.py',
    )

    load_delta = BashOperator(
        task_id='load_delta',
        bash_command='python /opt/project/scripts/demo/load_delta.py',
    )

    seed_kafka >> load_delta # >> start_simulation_code