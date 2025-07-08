from pyspark.sql import SparkSession
#from delta import configure_spark_with_delta_pip
from jobs.batch.process_raw_podcast import process
import os

from util.kafka import read_kafka_batch
#from util.delta import write_to_delta

KAFKA_TOPIC = os.getenv("TOPIC_RAW_PODCAST")

def run_raw_podcast_pipeline():
    print("Running raw podcast pipeline")
    df_raw = read_kafka_batch(KAFKA_TOPIC)
        # Convert binary 'value' to string
    df_string = df_raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")

    # Show messages (triggers the job)
    df_string.show(truncate=False, n=100)

    #print(df_raw)
    #processing logic
    #process(df_raw)
    #processing logic

