from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

from config.settings import (
    KAFKA_URL, TOPIC_USER_EVENTS_STREAMING, DELTA_PATH_DAILY,
    USER_EVENT_STREAM
)
from spark.util.common import get_spark

LIKE_W        = 3.0
COMPLETE_W    = 2.0
SKIP_W        = -1.0
PAUSE_SEC_CAP = 600.0

CONSUMER_GROUP_ID = 'user-events-streamer'

# ----------------- Spark Session -----------------
spark = get_spark("Streaming-user-events")

# ----------------- Schema -----------------
# Describe the JSON in Kafka. 
schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("episode_id", StringType()),
    StructField("event", StringType()),
    StructField("rating", IntegerType()),
    StructField("device", StringType()),
    StructField("ts", StringType()),
    StructField("position_sec", IntegerType()),
    StructField("from_sec", IntegerType()),
    StructField("to_sec", IntegerType()),
    StructField("played_pct", DoubleType()),
])

# ----------------- Read Kafka Stream -----------------
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_URL)
    .option("subscribe", TOPIC_USER_EVENTS_STREAMING)
    .option("kafka.group.id", CONSUMER_GROUP_ID)
    .option("startingOffsets", "earliest") #start from latest offsets
    .option("failOnDataLoss", "false")
    .load()
)

# ----------------- Parse JSON + Dedup -----------------
ts_ms = F.regexp_replace(
    F.col("ts"),
    r'\.(\d{3})\d+(?=(?:[+-]\d{2}:\d{2}|Z)$)',   # keep first 3 frac digits before timezone
    r'.\1'
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json")
       .select(F.from_json("json", schema).alias("e"))
       .select("e.*")
        .withColumn("ts", F.to_timestamp(ts_ms, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
       .withWatermark("ts", "2 hours")  # handle late events up to 2h
       .dropDuplicates(["event_id"])    # prevent double-counting
)

# ----------------- Compute Engagement Weights -----------------
# Map each event to a numeric "energy" contribution.
w = (
    F.when(F.col("event")=="like", F.lit(LIKE_W))
     .when(F.col("event")=="complete", F.lit(COMPLETE_W))
     .when((F.col("event")=="rate") & F.col("rating").isNotNull(), F.col("rating") - 3.0)
     .when(F.col("event")=="pause", F.least(F.coalesce(F.col("position_sec"), F.lit(0)) / PAUSE_SEC_CAP, F.lit(1.0)))
     .when(F.col("event")=="skip", F.lit(SKIP_W))
     .otherwise(F.lit(0.0))
)

scored = (
    parsed
    .withColumn("weight", F.greatest(w, F.lit(0.0)))
    .withColumn("day", F.to_date("ts"))
)

# ----------------- Process Each Micro-Batch -----------------
def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    daily = (
        batch_df.groupBy("user_id", "episode_id", "day")
                .agg(
                    F.sum("weight").alias("engagement"),
                    F.count(F.lit(1)).alias("num_events"),
                    F.max("ts").alias("last_ts")
                )
    )

    spark = batch_df.sparkSession

    # Initialize Delta table if it doesn't exist
    try:
        tgt = DeltaTable.forPath(spark, DELTA_PATH_DAILY)
    except Exception as e:
        print(f"[INFO] Creating Delta table: {e}")
        (daily.withColumn("created_at", F.current_timestamp())
              .withColumn("updated_at", F.current_timestamp())
              .write.format("delta")
              .partitionBy("day")
              .mode("overwrite")
              .save(DELTA_PATH_DAILY))
        tgt = DeltaTable.forPath(spark, DELTA_PATH_DAILY)

    # Merge new data into table (upsert)
    (
        tgt.alias("t")
           .merge(
               daily.alias("s"),
               "t.user_id = s.user_id AND t.episode_id = s.episode_id AND t.day = s.day"
           )
           .whenMatchedUpdate(set={
               "engagement": F.col("t.engagement") + F.col("s.engagement"),
               "num_events": F.col("t.num_events") + F.col("s.num_events"),
               "last_ts":    F.greatest(F.col("t.last_ts"), F.col("s.last_ts")),
               "updated_at": F.current_timestamp(),
           })
           .whenNotMatchedInsert(values={
               "user_id":    F.col("s.user_id"),
               "episode_id": F.col("s.episode_id"),
               "day":        F.col("s.day"),
                "engagement": F.col("s.engagement"),
               "num_events": F.col("s.num_events"),
               "last_ts":    F.col("s.last_ts"),
               "created_at": F.current_timestamp(),
               "updated_at": F.current_timestamp(),
           })
           .execute()
    )

# ----------------- Start Streaming Query -----------------
q = (
    scored.writeStream
          .foreachBatch(process_batch)
          .option("checkpointLocation", USER_EVENT_STREAM)
          .trigger(processingTime="2 seconds")    
          .start()
)

q.awaitTermination()
