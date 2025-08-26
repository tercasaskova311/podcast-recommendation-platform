from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="podcast_recommendations",
    start_date=days_ago(1),
    schedule_interval="@daily",  # or hourly
    catchup=False,
) as dag:

    generate_events = BashOperator(
        task_id="generate_user_events",
        bash_command="python /app/spark/pipelines/generate_user_events_pipeline.py"
    )

    train_model = BashOperator(
        task_id="train_als_model",
        bash_command="python /app/spark/pipelines/training_user_events_pipeline.py"
    )

    # Streaming pipeline is NOT included here.
    # It's deployed separately, always running.

    generate_events >> train_model
