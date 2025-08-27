
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from config.settings import SPARK_URL

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
       
    #PROCESS SIMILARITIES
    process_similarities = SparkSubmitOperator(
        task_id="process_similarities",
        application="/opt/project/spark/pipelines/analyze_transcripts_pipeline.py",
        name="process_similarities",
        packages="io.delta:delta-spark_2.12:3.1.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        conf={
            "spark.master": SPARK_URL,
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.session.timeZone": "UTC",
            "spark.driver.memory": "6g",
            "spark.driver.memoryOverhead": "2g",
        },
        env_vars={
            "SPARK_SUBMIT_MODE": "1",          # tells get_spark() not to set master/jars
            "PYTHONPATH": "/opt/project"
        },
        verbose=True,
    )

    #SIMULATE USER EVENTS
    simulate_user_events = BashOperator(
        task_id="simulate_user_events",
        bash_command=(
            "python3 /opt/project/scripts/streaming/user_events_simulation.py"
        )
    )

    load_delta >> process_similarities >> simulate_user_events