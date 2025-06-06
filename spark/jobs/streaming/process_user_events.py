# ==== STREAMING USER EVENT - CREATING ENGAGEMENT SCORE =======
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, current_timestamp
from pyspark.sql.types import IntegerType, StructType, StructField, StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("PodcastEngagementStreaming") \
    .getOrCreate()

# Define schema of the incoming data (e.g. user_id,podcast_id,likes,completions,skips)
# Adjust fields and types to match the Kafka raw message format after decoding
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("podcast_id", IntegerType(), True),
    StructField("likes", IntegerType(), True),
    StructField("completions", IntegerType(), True),
    StructField("skips", IntegerType(), True),
])

# === Read from Kafka ===
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "streaming") #initialize kafka topic
    .option("startingOffsets", "latest")
    .load()
)

# Kafka 'value' column is binary, decode to string = ok we adjust this later, idk what format are the data

decoded_stream = kafka_stream.selectExpr("CAST(value AS STRING) as csv_str")

# Split CSV string into columns
split_cols = split(col("csv_str"), ",")

parsed_stream = decoded_stream.select(
    split_cols.getItem(0).cast(IntegerType()).alias("user_id"),
    split_cols.getItem(1).cast(IntegerType()).alias("podcast_id"),
    split_cols.getItem(2).cast(IntegerType()).alias("likes"),
    split_cols.getItem(3).cast(IntegerType()).alias("completions"),
    split_cols.getItem(4).cast(IntegerType()).alias("skips"),
).withColumn("timestamp", current_timestamp())

# === Calculate engagement score ===
scored_stream = parsed_stream.withColumn(
    "engagement_score",
    0.5 * col("likes") + 0.3 * col("completions") - 0.2 * col("skips")
)

# === Aggregate engagement with watermarking ===
aggregated_scores = scored_stream \
    .withWatermark("timestamp", "1 hour") \
    .groupBy("user_id", "podcast_id") \
    .sum("engagement_score") \
    .withColumnRenamed("sum(engagement_score)", "engagement_score")

# === Write aggregated results to Delta Lake for downstream batch/stream consumption ===
query = aggregated_scores.writeStream \
    .outputMode("update") \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoints/engagement_agg") \
    .start("/tmp/engagement_aggregates")

query.awaitTermination()
