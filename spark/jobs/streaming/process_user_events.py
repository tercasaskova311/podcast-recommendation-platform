from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, current_timestamp, size
from pyspark.sql.types import IntegerType

# ==== Initialize Spark Session ==== 
spark = SparkSession.builder \
    .appName("PodcastEngagementStreaming") \
    .getOrCreate()

# ==== Read from Kafka ====
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "streaming")
    .option("startingOffsets", "latest")
    .load()
)

# ==== Decode Kafka value (binary to string) ====
decoded_stream = kafka_stream.selectExpr("CAST(value AS STRING) as csv_str")

# ==== Filter valid CSV rows (must have 5 fields) ====
validated_stream = decoded_stream.filter(
    size(split(col("csv_str"), ",")) == 5
)

# ==== Split CSV string into structured columns ====
split_cols = split(col("csv_str"), ",")

parsed_stream = validated_stream.select(
    split_cols.getItem(0).cast(IntegerType()).alias("user_id"),
    split_cols.getItem(1).cast(IntegerType()).alias("podcast_id"),
    split_cols.getItem(2).cast(IntegerType()).alias("likes"),
    split_cols.getItem(3).cast(IntegerType()).alias("completions"),
    split_cols.getItem(4).cast(IntegerType()).alias("skips"),
).withColumn("timestamp", current_timestamp())

# ==== Drop rows with nulls (safety net) ====
clean_stream = parsed_stream.dropna(subset=["user_id", "podcast_id", "likes", "completions", "skips"])

# ==== Compute Engagement Score ====
scored_stream = clean_stream.withColumn(
    "engagement_score",
    0.5 * col("likes") + 0.3 * col("completions") - 0.2 * col("skips")
)

# ==== Aggregate engagement with watermarking (per user + podcast) ====
aggregated_scores = scored_stream \
    .withWatermark("timestamp", "1 hour") \
    .groupBy("user_id", "podcast_id") \
    .sum("engagement_score") \
    .withColumnRenamed("sum(engagement_score)", "engagement_score")

# ==== Write to Delta Lake (for ALS consumption) ====
query = aggregated_scores.writeStream \
    .outputMode("update") \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoints/engagement_agg") \
    .start("/tmp/engagement_aggregates")

# === Keep the stream alive ===
query.awaitTermination()
