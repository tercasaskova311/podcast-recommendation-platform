from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import yaml

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
        conf={
            "spark.master": "local[*]",
            "spark.driver.memory": "6g",
            "spark.driver.memoryOverhead": "2g",
        },
        env_vars={"PYTHONPATH": "/opt/project"},
        verbose=True,
    )
    

    download_metadata >> get_transcripts >> process_similarities