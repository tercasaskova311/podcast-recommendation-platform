import os
from pyspark.sql import SparkSession, functions as F, Window

TEST_MODE = os.getenv("TEST_MODE", "0") == "1"

spark = (
    SparkSession.builder
    .appName("Final-Recommendations")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

if TEST_MODE:
    print("⚡ Running in TEST MODE - Using artificial data")
    # Mock ALS recommendations
    user_events_recs = spark.createDataFrame([
        ("u1", "e1", 0.9),
        ("u1", "e2", 0.7),
        ("u2", "e2", 0.8),
        ("u2", "e3", 0.6),
    ], ["user_id", "episode_id", "als_score"])

    # Mock transcript similarities
    transcripts_similarities = spark.createDataFrame([
        ("e1", "e4", 0.95),
        ("e1", "e5", 0.85),
        ("e2", "e6", 0.75),
        ("e3", "e7", 0.65),
    ], ["episode_id", "similar_episode_id", "similarity"])

else:
    # ----------------- Load ALS Recommendations from Mongo -----------------
    user_events_recs = (
        spark.read.format("mongo")
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", MONGO_COLLECTION_USER_EVENTS)
        .load()
    )

    # ----------------- Load Transcript Similarities from Mongo -----------------
    transcripts_similarities = (
        spark.read.format("mongo")
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", MONGO_COLLECTION)
        .load()
    )
# ----------------- Hybrid Recommendations -----------------
hybrid_recs = (
    user_events_recs.alias("als")
    .join(
        transcripts_similarities.alias("sim"),
        F.col("als.episode_id") == F.col("sim.episode_id"),
        how="left"
    )
    .select(
        F.col("als.user_id"),
        F.col("als.episode_id").alias("base_episode_id"),
        F.col("sim.similar_episode_id"),
        (
            F.col("als.als_score") * F.lit(0.7)
            + F.col("sim.similarity") * F.lit(0.3)
        ).alias("hybrid_score")
    )
    .dropna(subset=["similar_episode_id"])
)

# ----------------- Rank Top-N -----------------
window = Window.partitionBy("user_id").orderBy(F.desc("hybrid_score"))
final_recs = (
    hybrid_recs
    .withColumn("rank", F.row_number().over(window))
    .filter(F.col("rank") <= 3)
    .select("user_id", "similar_episode_id", "hybrid_score")
    .withColumnRenamed("similar_episode_id", "recommended_episode_id")
)

final_recs.show(truncate=False)
