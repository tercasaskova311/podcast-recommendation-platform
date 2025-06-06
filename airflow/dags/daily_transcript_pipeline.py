from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import yaml
from datetime import timedelta


with open('/opt/airflow/config/schedule_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='daily_transcript_pipeline',
    schedule_interval=config['download_transcripts_interval'],
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['batch'],
) as dag:

    wait_3_minutes = TimeDeltaSensor(
        task_id='wait_3_minutes',
        delta=timedelta(minutes=3),
    )
    
    init_episodes = BashOperator( 
        task_id='transcripts_downloader',
        bash_command='python /opt/airflow/scripts/transcriptions/transcriptions.py',
    )

    transcripts_downloader = BashOperator(
        task_id='transcripts_downloader',
        bash_command='python /opt/airflow/scripts/transcriptions/transcriptions.py',
    )

    run_raw_episodes_processing = SparkSubmitOperator(
        task_id='raw_episodes_processing',
        application='/opt/airflow/spark_jobs/main.py',
        application_args=['--job', 'raw-episode'],
        conn_id='spark_default',
        conf={'spark.master': 'spark://spark-master:7077'},
    )

#We wait 3 minutes allowing the 
    wait_3_minutes >> init_episodes >> run_raw_episodes_processing >> transcripts_downloader >> run_raw_episodes_processing