from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import yaml

with open('/opt/airflow/config/schedule_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='test',
    start_date = days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['batch'],
) as dag:

    #PROCESS USER EVENTS, FINDING SIMILARITIES
    process_similarities = SparkSubmitOperator(
        task_id="process_similarities",
        application="/opt/project/spark/pipelines/training_user_events_pipeline.py",
        name="process_similarities",
        packages="io.delta:delta-spark_2.12:3.1.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        conf={
            "spark.master": "spark://spark-master:7077",
        },
        env_vars={"PYTHONPATH": "/opt/project"},
        verbose=True,
    )
    

    process_similarities