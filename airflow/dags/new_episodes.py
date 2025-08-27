from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import yaml

from config.settings import SPARK_URL

with open('/opt/airflow/config/schedule_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='new_episodes',
    start_date = days_ago(1),
    schedule_interval=config['new_episodes_interval'],
    catchup=False,
    tags=['batch'],
) as dag:
    
    #DOWNLOAD NEW ESPISODES AND SEND THEM TO KAFKA
    download_metadata = BashOperator(
        task_id='download_metadata',
        bash_command='python3 /opt/project/scripts/batch/new_episodes_download.py',
    )

    #PROCESS TRANSCRIPTS AND STORE THEM IN DELTA
    get_transcripts = BashOperator(
        task_id='get_transcripts',
        bash_command='python3 /opt/project/scripts/batch/new_episodes_get_transcripts.py'
    )

    #PROCESS SIMILARITIES
    process_similarities = SparkSubmitOperator(
        task_id="process_similarities",
        application="/opt/project/spark/pipelines/analyze_transcripts_pipeline.py",
        name="process_similarities",
        packages="io.delta:delta-spark_2.12:3.1.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        conf={
            "spark.master": SPARK_URL,
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.session.timeZone": "UTC",
            "spark.driver.memory": "6g",
            "spark.driver.memoryOverhead": "2g",
        },
        env_vars={
            "SPARK_SUBMIT_MODE": "1",          # tells get_spark() not to set master/jars
            "PYTHONPATH": "/opt/project"
        },
        verbose=True,
    )

    download_metadata >> get_transcripts >> process_similarities