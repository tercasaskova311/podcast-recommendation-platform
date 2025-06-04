from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import yaml

with open('/opt/airflow/config/schedule_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='daily_transcript_pipeline',
    schedule_interval=config['download_transcripts_interval'],
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['batch'],
) as dag:

    download_transcripts = BashOperator(
        task_id='download_transcripts',
        bash_command='python /opt/airflow/scripts/downloader.py', #terka's script
    )

    run_transcripts_processing = SparkSubmitOperator(
        task_id='transcripts_processing',
        application='/opt/airflow/spark_jobs/main.py',
        application_args=['--job', 'transcripts-en'],
        conn_id='spark_default',
        conf={'spark.master': 'spark://spark-master:7077'},
    )

    run_metadata_processing = SparkSubmitOperator(
        task_id='metadata_processing',
        application='/opt/airflow/spark_jobs/main.py',
        application_args=['--job', 'metadata'],
        conn_id='spark_default',
        conf={'spark.master': 'spark://spark-master:7077'},
    )

    download_transcripts >> [run_transcripts_processing, run_metadata_processing]