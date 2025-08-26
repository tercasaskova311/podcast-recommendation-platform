from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from config.settings import SPARK_URL
import yaml

with open('/opt/airflow/config/schedule_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='test',
    start_date = days_ago(1),
    schedule_interval=config['analyze_transcripts'],
    catchup=False,
    tags=['batch'],
) as dag:
    
    analyze = BashOperator(
        task_id='test',
        bash_command='export SPARK_SUBMIT_MODE python3 /opt/project/spark/pipelines/analyze_transcripts_pipeline.py',
        env={
            "SPARK_SUBMIT_MODE": "0",
            "PYTHONPATH": "/opt/project"
        },
    )


    analyze