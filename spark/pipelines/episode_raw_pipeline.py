from pyspark import SparkSession
from delta import configure_spark_with_delta_pip
from jobs.batch.process_episode_raw import process_episode_raw

from util.kafka import read_kafka_batch
from util.delta import write_to_delta

def run_transcripts_en_pipeline():
    print("Running transcripts-en batch job")
    #df_raw = read_kafka_batch("transcripts-en")

    #processing logic

