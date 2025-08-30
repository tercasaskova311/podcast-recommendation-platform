from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pymongo import MongoClient, UpdateOne, ASCENDING

from config.settings import (
    KAFKA_URL, TOPIC_USER_EVENTS_STREAMING, DELTA_PATH_DAILY,
    USER_EVENT_STREAM,
    MONGO_URI, MONGO_DB, MONGO_COLLECTION_USER_HISTORY, DELTA_RAW_USER_EVENTS_PATH,
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
    StructField("bucket", StringType()),  # optional if present from producer
])

# ----------------- Read Kafka Stream -----------------
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_URL)
    .option("subscribe", TOPIC_USER_EVENTS_STREAMING)
    .option("kafka.group.id", CONSUMER_GROUP_ID)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "200")
    .option("failOnDataLoss", "false")
    .load()
)

# ----------------- Parse JSON + Dedup -----------------
# keep only 3 fractional ms digits (Spark format "SSS")
ts_ms = F.regexp_replace(
    F.col("ts"),
    r'\.(\d{3})\d+(?=(?:[+-]\d{2}:\d{2}|Z)$)',
    r'.\1'
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json")
       .select(F.from_json("json", schema).alias("e"))
       .select("e.*")
       .withColumn("ts", F.to_timestamp(ts_ms, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
       .withWatermark("ts", "2 hours")
       .dropDuplicates(["event_id"])
)

# Raw events sink for dashboard
raw_events_for_delta = (
    parsed
    .select("ts", "user_id", "episode_id", "event", "rating", "event_id", "device")
    .withColumn("day", F.to_date("ts"))
)
# ----------------- Compute Engagement Weights -----------------
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

# Init Delta daily table if missing
try:
    DeltaTable.forPath(spark, DELTA_PATH_DAILY)
except Exception:
    (spark.createDataFrame([], """
        user_id string, episode_id string, day date,
        engagement double, num_events long, last_ts timestamp,
        created_at timestamp, updated_at timestamp
    """)
     .write.format("delta").partitionBy("day").mode("overwrite").save(DELTA_PATH_DAILY))
    
# Init Delta raw user events if missing
try:
    DeltaTable.forPath(spark, DELTA_RAW_USER_EVENTS_PATH)
except Exception:
    (spark.createDataFrame([], "ts timestamp, user_id string, episode_id string, event string, rating int, event_id string, device string")
         .withColumn("day", F.to_date(F.col("ts")))
         .write.format("delta").partitionBy("day").mode("overwrite").save(DELTA_RAW_USER_EVENTS_PATH))
    
# ----------------- Process Each Micro-Batch -----------------
def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # 1) Upsert daily aggregates to Delta
    daily = (
        batch_df.groupBy("user_id", "episode_id", "day")
                .agg(
                    F.sum("weight").alias("engagement"),
                    F.count(F.lit(1)).alias("num_events"),
                    F.max("ts").alias("last_ts")
                )
    )

    spark = batch_df.sparkSession
    tgt = DeltaTable.forPath(spark, DELTA_PATH_DAILY)

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

    # 2) Build "history" candidates (positive-interest rule)
    #    Rule: include if like OR complete OR (rate >= 4). Keep most recent ts per pair.
    hist_candidates = (
        batch_df
        .filter(
            (F.col("event") == F.lit("like")) |
            (F.col("event") == F.lit("complete")) |
            ((F.col("event") == F.lit("rate")) & (F.col("rating") >= F.lit(4)))
        )
        .groupBy("user_id", "episode_id")
        .agg(F.max("ts").alias("last_ts"))
        .dropna(subset=["user_id", "episode_id"])
    )

    # Collect small distinct set and upsert to Mongo (idempotent)
    pairs = hist_candidates.select("user_id", "episode_id", "last_ts").toLocalIterator()

    client = MongoClient(MONGO_URI)
    coll = client[MONGO_DB][MONGO_COLLECTION_USER_HISTORY]
    # Ensure unique (first run is cheap; subsequent runs are no-ops)
    coll.create_index([("user_id", ASCENDING), ("episode_id", ASCENDING)], unique=True)

    ops = []
    for row in pairs:
        ops.append(
            UpdateOne(
                {"user_id": row["user_id"], "episode_id": row["episode_id"]},
                {
                    "$setOnInsert": {"first_seen_at": row["last_ts"]},
                    "$set": {"last_seen_at": row["last_ts"]}
                },
                upsert=True
            )
        )

    if ops:
        coll.bulk_write(ops, ordered=False)

# ----------------- Start Streaming Query -----------------
raw_q = (
    raw_events_for_delta.writeStream
        .format("delta")
        .option("path", DELTA_RAW_USER_EVENTS_PATH)
        .option("checkpointLocation", f"{USER_EVENT_STREAM}/raw")  # separate checkpoint
        .outputMode("append")
        .partitionBy("day")
        .start()
)
agg_q = (
    scored.writeStream
          .foreachBatch(process_batch)
          .option("checkpointLocation", f"{USER_EVENT_STREAM}/agg") # separate checkpoint
          .trigger(processingTime="30 seconds")
          .start()
)

spark.streams.awaitAnyTermination()