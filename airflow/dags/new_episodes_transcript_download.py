from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import yaml

with open('/opt/airflow/config/schedule_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='new_episodes_transcript_download',
    start_date = days_ago(1),
    schedule_interval=config['new_episodes_transcript_download_interval'],
    catchup=False,
    tags=['batch', 'download'],
) as dag:
    
    new_episodes_download = BashOperator(
        task_id='new_episodes_download',
        bash_command='python3 /opt/project/scripts/batch/new_episodes_download.py',
    )
    

    new_episodes_get_transcripts = BashOperator(
        task_id='new_episodes_get_transcripts',
        bash_command='python3 /opt/project/scripts/batch/new_episodes_get_transcripts.py'
    )

    new_episodes_download >> new_episodes_get_transcripts