from pyspark.sql import SparkSession

from util.kafka import read_kafka_batch
from util.delta import write_to_delta

def run_streaming_pipeline():
    print("Running streaming pipeline")
    #df_raw = read_kafka_batch("transcripts-en")

    #processing logic

