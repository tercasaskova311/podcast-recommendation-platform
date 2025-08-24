# spark/jobs/ingest_user_events_stream.py
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp

KAFKA_SERVERS = "kafka1:9092"
TOPIC = "user-events-streaming"
DELTA_PATH_EVENTS = "/data/delta/user_events"

schema = StructType([
    StructField("event_id", StringType()), 
    StructField("user_id", StringType()),
    StructField("episode_id", StringType()),
    StructField("event", StringType()),
    StructField("rating", IntegerType()),
    StructField("device", StringType()),
    StructField("ts", StringType()),
])

spark = (SparkSession.builder
         .appName("IngestUserEvents")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_SERVERS)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "latest")
       .load())

parsed = raw.selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("e")) \
            .select("e.*") \
            .withColumn("ts", to_timestamp("ts")) \
            .withColumn("ingested_at", current_timestamp())

def write_delta(batch_df, batch_id):
    (batch_df
        .write
        .format("delta")
        .mode("append")
        .save(DELTA_PATH_EVENTS))

query = (parsed.writeStream
         .foreachBatch(write_delta)
         .option("checkpointLocation", "/chk/user_events")
         .outputMode("append")
         .start())

query.awaitTermination()
