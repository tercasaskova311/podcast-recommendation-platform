from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import yaml
import os

# Now use the environment variables
SPARK_URL= os.getenv("SPARK_URL")

with open('/opt/airflow/config/schedule_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='new_episodes_transcript_download',
    start_date = datetime(2025, 6, 9),
    schedule_interval=config['new_episodes_transcript_download_interval'],
    catchup=False,
    tags=['batch'],
) as dag:
    
    new_episodes_download = BashOperator(
        task_id='new_episodes_download',
        bash_command='python /opt/scripts/batch/new_episodes_download.py',
    )

    new_episodes_get_transcripts = SparkSubmitOperator(
        task_id='new_episodes_get_transcripts',
        application='/opt/spark_jobs/main.py',
        application_args=['--job', 'download-transcripts-pipeline'],
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,io.delta:delta-spark_2.12:3.1.0',
        conf={
            "spark.master": SPARK_URL,
            "spark.submit.deployMode": "client",
            "spark.driverEnv.PYTHONPATH": "/opt/spark_jobs", 
            "spark.executorEnv.PYTHONPATH": "/opt/spark_jobs",
            "spark.executor.memoryOverhead": 1024,
            "spark.network.timeout": 600,
            "spark.executor.heartbeatInterval": 60
        },
        name="new_episodes_get_transcripts",
        driver_memory="2g",
        executor_memory="2g",
        env_vars={
            'PYTHONPATH': '/opt/spark_jobs',
            'JAVA_HOME': '/usr/lib/jvm/java-17-openjdk-amd64'
        }
    )

    new_episodes_download >> new_episodes_get_transcripts