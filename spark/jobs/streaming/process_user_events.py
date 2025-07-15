from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, current_timestamp, size
from pyspark.sql.types import IntegerType

# ========== CONFIG ==========
KAFKA_TOPIC = "streaming"
KAFKA_SERVERS = "localhost:9092"
DELTA_OUTPUT_PATH = "/tmp/engagement_aggregates"
CHECKPOINT_DIR = "/tmp/checkpoints/engagement_agg"


# ========== SPARK SESSION ==========
spark = SparkSession.builder \
    .appName("PodcastEngagementStreaming") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)
decoded_stream = kafka_stream.selectExpr("CAST(value AS STRING) as csv_str")

validated_stream = decoded_stream.filter(
    size(split(col("csv_str"), ",")) == 5
)

split_cols = split(col("csv_str"), ",")

parsed_stream = validated_stream.select(
    split_cols.getItem(0).cast(IntegerType()).alias("user_id"),
    split_cols.getItem(1).cast(IntegerType()).alias("episode_id"),
    split_cols.getItem(2).cast(IntegerType()).alias("likes"),
    split_cols.getItem(3).cast(IntegerType()).alias("completions"),
    split_cols.getItem(4).cast(IntegerType()).alias("skips"),
).withColumn("timestamp", current_timestamp())

clean_stream = parsed_stream.dropna(subset=["user_id", "episode_id", "likes", "completions", "skips"])

# ==== ENGAGEMENT SCORE ====
scored_stream = clean_stream.withColumn(
    "engagement_score",
    0.5 * col("likes") + 0.3 * col("completions") - 0.2 * col("skips")
)

# ==== AGGREGATE ====
aggregated_scores = scored_stream \
    .withWatermark("timestamp", "2 days") \
    .groupBy("user_id", "episode_id") \
    .sum("engagement_score") \
    .withColumnRenamed("sum(engagement_score)", "engagement_score")

# ==== TO DELTA LAKE ====
query = aggregated_scores.writeStream \
    .outputMode("update") \
    .format("delta") \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start(DELTA_OUTPUT_PATH)

# === Keep the stream alive ===
query.awaitTermination()


