from util.common import get_spark
import os

KAFKA_URL = os.getenv("KAFKA_URL")

def read_kafka_batch(topic):
    spark = get_spark()
    return spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_URL) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

def read_kafka_stream(topic):
    spark = get_spark()
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_URL) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()