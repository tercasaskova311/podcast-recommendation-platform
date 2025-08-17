
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

SPARK_URL= os.getenv("SPARK_URL")

with DAG('demo_transcript_pipeline',
        schedule_interval=None,
        start_date = datetime(2025, 1, 1),
        catchup=False,
        tags=['batch']
) as dag:

    seed_kafka = BashOperator(
        task_id='seed_kafka',
        bash_command='python /opt/scripts/demo/seed_kafka.py',
    )

    load_delta = SparkSubmitOperator(
        task_id='load_delta',
        application='/opt/scripts/demo/load_delta.py',
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
        name="load_delta",
        driver_memory="2g",
        executor_memory="2g",
        env_vars={
            'PYTHONPATH': '/opt/spark_jobs',
            'JAVA_HOME': '/usr/lib/jvm/java-17-openjdk-amd64'
        }
    )

    seed_kafka >> load_delta # >> start_simulation_code