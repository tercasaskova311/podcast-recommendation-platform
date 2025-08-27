from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="podcast_recommendations",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",  # Run once a day
    catchup=False,
    tags=["recommendations", "spark"],
) as dag:

    generate_events = BashOperator(
        task_id="generate_user_events",
        bash_command=(
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark_jobs/spark/pipelines/generate_user_events_pipeline.py"
        ),
    )

    train_model = BashOperator(
        task_id="train_user_events",
        bash_command=(
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark_jobs/spark/pipelines/training_user_events_pipeline.py"
        ),
    )

    generate_events >> train_model
    # If streaming already produces data, just run: train_model
