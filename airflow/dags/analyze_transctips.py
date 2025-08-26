from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config.settings import SPARK_URL
import yaml

with open('/opt/airflow/config/schedule_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='analyze_transcripts',
    start_date = days_ago(1),
    schedule_interval=config['analyze_transcripts'],
    catchup=False,
    tags=['batch'],
) as dag:
    
    analyze = SparkSubmitOperator(
        task_id="analyze",
        application="/opt/project/spark/pipelines/analyze_transcripts_pipeline.py",
        name="analyze-transcripts",
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


    analyze