from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import yaml
import os

# Now use the environment variables
KAFKA_URL = os.getenv("KAFKA_URL")
TOPIC_RAW_PODCAST = os.getenv("TOPIC_RAW_PODCAST")
SPARK_URL= os.getenv("SPARK_URL")

with open('/opt/airflow/config/schedule_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='daily_transcript_pipeline',
    schedule_interval=config['download_transcripts_interval'],
    catchup=False,
    tags=['batch'],
) as dag:
    
    transcripts_downloader = BashOperator(
        task_id='transcripts_downloader',
        bash_command='python /opt/airflow/scripts/transcriptions/transcriptions.py',
    )

    process_raw_podcast = SparkSubmitOperator(
        task_id='process_raw_podcast',
        application='/opt/airflow/spark_jobs/main.py',
        application_args=['--job', TOPIC_RAW_PODCAST],
        conn_id='spark_default',
        conf={'spark.master': SPARK_URL},
    )

    transcripts_downloader >> process_raw_podcast