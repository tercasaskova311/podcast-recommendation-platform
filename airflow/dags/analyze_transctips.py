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
    dag_id='analyze_transcripts',
    start_date = datetime(2025, 6, 9),
    schedule_interval=config['analyze_transcripts'],
    catchup=False,
    tags=['batch'],
) as dag:

    analyze_transcripts = SparkSubmitOperator(
        task_id='analyze_transcripts',
        application='/opt/spark_jobs/main.py',
        application_args=['--job', 'analyze-transcripts-pipeline'],
        packages='io.delta:delta-spark_2.12:3.1.0',
        conf={
            "spark.master": SPARK_URL,
            "spark.sql.shuffle.partitions": "64",
            "spark.submit.deployMode": "client",
            "spark.driverEnv.PYTHONPATH": "/opt/spark_jobs", 
            "spark.executorEnv.PYTHONPATH": "/opt/spark_jobs",
            "spark.executor.memoryOverhead": 1024,
            "spark.network.timeout": 600,
            "spark.executor.heartbeatInterval": 60
        },
        name="analyze_transcripts",
        driver_memory="2g",
        executor_memory="2g",
        env_vars={
            'PYTHONPATH': '/opt/spark_jobs',
            'JAVA_HOME': '/usr/lib/jvm/java-17-openjdk-amd64'
        }
    )
    analyze_transcripts