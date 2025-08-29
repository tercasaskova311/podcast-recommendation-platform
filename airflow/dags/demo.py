
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

from config.settings import N_EVENTS_PER_USER

with DAG('demo',
        schedule_interval=None,
        start_date = days_ago(1),
        catchup=False,
        tags=['batch']
) as dag:

    #LOAD SOME SAMPLE DATA IN DELTA LAKE
    load_delta = BashOperator(
        task_id='load_delta',
        bash_command='python /opt/project/scripts/demo/load_delta.py',
    )

    load_users = BashOperator(
        task_id='load_users',
        bash_command='python /opt/project/scripts/demo/load_users.py',
    )
       
    #PROCESS SIMILARITIES
    process_similarities = SparkSubmitOperator(
        task_id="process_similarities",
        application="/opt/project/spark/pipelines/analyze_transcripts_pipeline.py",
        name="process_similarities",
        packages="io.delta:delta-spark_2.12:3.1.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        conf={
            "spark.master": "local[*]",
            "spark.driver.memory": "4g",
            "spark.driver.memoryOverhead": "1g",
        },
        env_vars={"PYTHONPATH": "/opt/project"},
        verbose=True,
    )
    

    #SIMULATE USER EVENTS - we do some iteration to generate a some data
    with TaskGroup("simulate_user_events") as simulate_user_events:
        for i in range(N_EVENTS_PER_USER):
            BashOperator(
                task_id=f"run_{i}",
                bash_command="python3 /opt/project/scripts/streaming/user_events_simulation.py",
            )

    load_delta >> load_users >> process_similarities >> simulate_user_events