from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, current_timestamp, size
from pyspark.sql.types import IntegerType
import logging

# ========== CONFIG ==========
KAFKA_TOPIC = "streaming"
KAFKA_SERVERS = "localhost:9092"
MONGO_URI = "mongodb://localhost:27017/engagement_score.user_events"
CHECKPOINT_DIR = "/tmp/checkpoints/engagement_agg"

# ========== SPARK SESSION ==========
spark = SparkSession.builder \
    .appName("PodcastEngagementStreaming") \
    .config("spark.sql.shuffle.partitions", "200")  # Increased shuffle partitions for scalability
    .config("spark.mongodb.output.uri", MONGO_URI)  # MongoDB integration
    .getOrCreate()
    

# Set up logging
logging.basicConfig(level=logging.INFO)

# ========== STREAMING SETUP ==========
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Decode and validate the stream
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

# Compute engagement score and clean data
clean_stream = parsed_stream.dropna(subset=["user_id", "episode_id", "likes", "completions", "skips"])

# ==== ENGAGEMENT SCORE ====
scored_stream = clean_stream.withColumn(
    "engagement_score",
    0.5 * col("likes") + 0.3 * col("completions") - 0.2 * col("skips")
)

# ==== TO MONGO DB ====
query = scored_stream.writeStream \
    .outputMode("append") \
    .format("mongo") \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

# === Keep the stream alive ===
query.awaitTermination()
