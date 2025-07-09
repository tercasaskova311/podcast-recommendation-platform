from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
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
    start_date = datetime(2025, 6, 9),
    schedule_interval=config['download_transcripts_interval'],
    catchup=False,
    tags=['batch'],
) as dag:
    
    new_episodes_downloader = BashOperator(
        task_id='new_episodes_downloader',
        bash_command='python /opt/scripts/batch/new_episodes_downloader.py',
    )

    process_raw_podcast = SparkSubmitOperator(
        task_id='process_raw_podcast',
        application='/opt/spark_jobs/main.py',
        application_args=['--job', TOPIC_RAW_PODCAST],
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,io.delta:delta-spark_2.12:3.1.0',
        conf={
            "spark.master": SPARK_URL,
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        env_vars={
            'PYTHONPATH': '/opt/spark_jobs',
            'JAVA_HOME': '/usr/lib/jvm/java-17-openjdk-amd64'
        }
    )

    new_episodes_downloader >> process_raw_podcast