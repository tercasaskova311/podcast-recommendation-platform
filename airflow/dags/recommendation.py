from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
    process_users_events = SparkSubmitOperator(
        task_id="process_users_events",
        application="/opt/project/spark/pipelines/training_user_events_pipeline.py",
        name="process_users_events",
        packages="io.delta:delta-spark_2.12:3.1.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        conf={
            "spark.master": "local[*]",
            "spark.driver.memory": "4g",
            "spark.driver.memoryOverhead": "1g",
        },
        env_vars={"PYTHONPATH": "/opt/project"},
        verbose=True,
    )

    #RETURN FINAL RACOMMENDATION TO USERS
    user_recommendation = SparkSubmitOperator(
        task_id="user_recommendation",
        application="/opt/project/spark/pipelines/final_recommendation.py",
        name="user_recommendation",
        packages="io.delta:delta-spark_2.12:3.1.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        conf={
            "spark.master": "local[*]",
            "spark.driver.memory": "4g",
            "spark.driver.memoryOverhead": "1g",
        },
        env_vars={"PYTHONPATH": "/opt/project"},
        verbose=True,
    )

    process_users_events >> user_recommendation