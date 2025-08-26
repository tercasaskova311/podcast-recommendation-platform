from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from spark.config.settings import (
    ALS_MODEL_PATH, TOP_N,
    DELTA_PATH_DAILY,
    DELTA_PATH_SIMILARITIES, MONGO_DB, MONGO_COLLECTION,
    MONGO_COLLECTION_USER_EVENTS, MONGO_URI,
    MONGO_COLLECTION_FINAL_RECS
)

# ----------------- Spark Session -----------------
spark = (
    SparkSession.builder
    .appName("Final-Recommendations")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# ----------------- 1. Load ALS Recommendations -----------------
user_events_recs = (
    spark.read.format("mongo")
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", MONGO_COLLECTION_USER_EVENTS)
        .load()
)

# ----------------- 2. Load Transcript Similarities -----------------
transcripts_similarities = (
    spark.read.format("mongo")
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", MONGO_COLLECTION)
        .load()
        .withColumnRenamed("new_episode_id", "episode_id")
        .withColumnRenamed("historical_episode_id", "similar_episode_id")
)

# ----------------- 3. Enrich ALS Recommendations -----------------
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

# ----------------- 4. Rank Top-N Final Recommendations -----------------
window = Window.partitionBy("user_id").orderBy(F.desc("hybrid_score"))

final_recs = (
    hybrid_recs
    .withColumn("rank", F.row_number().over(window))
    .filter(F.col("rank") <= TOP_N)
    .select("user_id", "similar_episode_id", "hybrid_score")
    .withColumnRenamed("similar_episode_id", "recommended_episode_id")
)

# ----------------- 5. Save to MongoDB -----------------
(final_recs.write
    .format("mongo")
    .mode("overwrite")  # or 'append' if you want historical snapshots
    .option("uri", MONGO_URI)
    .option("database", MONGO_DB)
    .option("collection", MONGO_COLLECTION_FINAL_RECS)
    .save()
)

spark.stop()
