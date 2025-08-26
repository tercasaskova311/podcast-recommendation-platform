from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

from spark.config.settings import (
    KAFKA_SERVERS, TOPIC_USER_EVENTS_STREAMING, DELTA_PATH_DAILY,
    USER_EVENT_STREAM,
    LIKE_W, COMPLETE_W, SKIP_W, PAUSE_SEC_CAP
)

# ----------------- Spark Session -----------------
spark = (
    SparkSession.builder
    .appName("Streaming-user-events")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

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
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", TOPIC_USER_EVENTS_STREAMING)
    .option("startingOffsets", "latest") #start from latest offsets
    .option("failOnDataLoss", "false")
    .load()
)

# ----------------- Parse JSON + Dedup -----------------
parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json")
       .select(F.from_json("json", schema).alias("e"))
       .select("e.*")
       .withColumn("ts", F.to_timestamp("ts"))
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
        print(f"[INFO] Creating Silver Delta table: {e}")
        (daily.withColumn("created_at", F.current_timestamp())
              .withColumn("updated_at", F.current_timestamp())
              .write.format("delta")
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
