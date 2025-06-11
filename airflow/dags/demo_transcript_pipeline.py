
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

from dotenv import load_dotenv

# Load the .env file
load_dotenv('/opt/airflow/.env.development')

# Now use the environment variables
KAFKA_URL = os.getenv("KAFKA_URL")
TOPIC_RAW_PODCAST = os.getenv("TOPIC_RAW_PODCAST")
SPARK_URL= os.getenv("SPARK_URL")

with DAG('demo_transcript_pipeline',
        schedule_interval=None,
        start_date = datetime(2025, 6, 9),
        catchup=False) as dag:

    load_bootstrap_transcriptions = BashOperator(
        task_id='load_bootstrap_transcriptions',
        bash_command='python3 /opt/airflow/scripts/demo/bootstrap_transcriptions.py',
    )

    process_raw_podcast = SparkSubmitOperator(
        task_id='process_raw_podcast',
        application='/opt/airflow/spark_jobs/main.py',
        application_args=['--job', TOPIC_RAW_PODCAST],
        conn_id='spark_default',
        conf={'spark.master': SPARK_URL},
    )

    #start the user-event simulation script

    load_bootstrap_transcriptions >> process_raw_podcast
