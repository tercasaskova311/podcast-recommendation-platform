# spark/jobs/ingest_user_events_stream.py
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import math


from spark.config.settings import (
    KAFKA_SERVERS, TOPIC_USER_EVENTS_STREAMING,
    DELTA_PATH_EVENTS as BRONZE,
    DELTA_PATH_DAILY  as SILVER,
    CHK_PATH          as CHK,
    LIKE_W, COMPLETE_W, SKIP_W, PAUSE_SEC_CAP, DECAY_HALFLIFE_DAYS,
)

# ingest_user_events_stream.py
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("IngestUserEvents->Bronze+Silver")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)


schema = StructType([
    StructField("event_id",    StringType()),
    StructField("user_id",     StringType()),
    StructField("episode_id",  StringType()),
    StructField("event",       StringType()),
    StructField("rating",      IntegerType()),
    StructField("device",      StringType()),
    StructField("ts",          StringType()),
    StructField("position_sec", IntegerType()),
    StructField("from_sec",     IntegerType()),
    StructField("to_sec",       IntegerType()),
    StructField("played_pct",   DoubleType()),
])

raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", TOPIC_USER_EVENTS_STREAMING)
    .option("startingOffsets", "earliest")
    .load())

parsed = (raw.selectExpr("CAST(value AS STRING) AS json")
    .select(F.from_json("json", schema).alias("e")).select("e.*")
    .withColumn("ts", F.to_timestamp("ts"))
    .withWatermark("ts", "2 hours")
    .dropDuplicates(["event_id"]))

# weights (nonnegative for ALS)
w = (F.when(F.col("event")=="like", F.lit(LIKE_W))
     .when(F.col("event")=="complete", F.lit(COMPLETE_W))
     .when((F.col("event")=="rate") & F.col("rating").isNotNull(), F.col("rating") - 3.0)
     .when(F.col("event")=="pause", F.least(F.coalesce(F.col("position_sec"), F.lit(0))/PAUSE_SEC_CAP, F.lit(1.0)))
     .when(F.col("event")=="skip", F.lit(SKIP_W))
     .otherwise(F.lit(0.0)))

# optional recency decay (half-life in days)
if DECAY_HALFLIFE_DAYS and DECAY_HALFLIFE_DAYS > 0:
    ln2 = math.log(2.0)  # python constant is fine here
    age_days = (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("ts")) / F.lit(86400.0)
    decay = F.exp(-F.lit(ln2) * age_days / F.lit(float(DECAY_HALFLIFE_DAYS)))
else:
    decay = F.lit(1.0)


scored = (parsed
    .withColumn("weight", F.greatest(w * decay, F.lit(0.0)))
    .withColumn("day", F.to_date("ts")))

def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # 1) Bronze (append raw)
    (batch_df.drop("day","weight")
        .write.format("delta").mode("append").save(BRONZE))

    # 2) Silver (daily agg + MERGE)
    daily = (batch_df.groupBy("user_id","episode_id","day")
             .agg(F.sum("weight").alias("engagement"),
                  F.count(F.lit(1)).alias("num_events"),
                  F.max("ts").alias("last_ts")))
    spark = batch_df.sparkSession
    try:
        tgt = DeltaTable.forPath(spark, SILVER)
    except:
        (daily.withColumn("created_at", F.current_timestamp())
              .withColumn("updated_at", F.current_timestamp())
              .write.format("delta").mode("overwrite").save(SILVER))
        tgt = DeltaTable.forPath(spark, SILVER)

    (tgt.alias("t")
     .merge(daily.alias("s"),
            "t.user_id=s.user_id AND t.episode_id=s.episode_id AND t.day=s.day")
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
     .execute())

q = (scored.writeStream
     .foreachBatch(process_batch)
     .option("checkpointLocation", CHK)
     .outputMode("update")
     .start())
q.awaitTermination()
