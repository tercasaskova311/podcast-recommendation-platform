from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import yaml

with open('/opt/airflow/config/schedule_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='recommendation',
    start_date = days_ago(1),
    schedule_interval=config['recommendation_interval'],
    catchup=False,
    tags=['batch', 'user behaviour', 'content based'],
) as dag:

    #PROCESS USER EVENTS, FINDING SIMILARITIES
    process_users_events = BashOperator(
        task_id='process_users_events',
        bash_command='python /opt/project/spark/pipelines/training_user_events_pipeline.py',
    )

    #RETURN FINAL RACOMMENDATION TO USERS
    user_recommendation = BashOperator(
        task_id='user_recommendation',
        bash_command='python /opt/project/spark/pipelines/final_recommendation.py',
    )

    process_users_events >> user_recommendation