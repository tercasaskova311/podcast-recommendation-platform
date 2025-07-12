
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

# Now use the environment variables
TOPIC_RAW_PODCAST = os.getenv("TOPIC_RAW_PODCAST")
SPARK_URL= os.getenv("SPARK_URL")
KAFKA_URL = os.getenv("KAFKA_URL")

with DAG('demo_transcript_pipeline',
        schedule_interval=None,
        start_date = datetime(2025, 6, 9),
        catchup=False,
        tags=['batch']
) as dag:

    load_bootstrap_transcriptions = BashOperator(
        task_id='load_bootstrap_transcriptions',
        bash_command='python /opt/scripts/demo/bootstrap_transcriptions.py',
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

    #start the user-event simulation script

    load_bootstrap_transcriptions >> process_raw_podcast
