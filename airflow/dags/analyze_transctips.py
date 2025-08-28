from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import yaml

with open('/opt/airflow/config/schedule_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='debug_analyze_transcripts',
    start_date = days_ago(1),
    schedule_interval=config['new_episodes_interval'],
    catchup=False,
    tags=['batch', 'debug'],
) as dag:

    analyze = SparkSubmitOperator(
        task_id="analyze",
        application="/opt/project/spark/pipelines/analyze_transcripts_pipeline.py",
        name="analyze-transcripts",
        conf={
            "spark.master": "local[*]",
            "spark.driver.memory": "6g",
            "spark.driver.memoryOverhead": "2g",
        },
        env_vars={"PYTHONPATH": "/opt/project"},
        verbose=True,
    )


    analyze