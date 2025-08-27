# spark/pipelines/final_recommendation.py
# Minimal hybrid recommender (ALS × content) using legacy Mongo connector ("mongo")

import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

from spark.config.settings import (
    TOP_N,
    MONGO_URI, MONGO_DB,
    MONGO_COLLECTION_USER_EVENTS,   # ALS seeds
    MONGO_COLLECTION,               # content neighbors
    MONGO_COLLECTION_FINAL_RECS, 
)


EXTRA_PKGS = os.getenv("SPARK_PACKAGES", "")  # e.g., Kafka

spark = (
    SparkSession.builder
    .appName("Final-Recommendations")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# --- 1) Read ALS seeds (user_id, episode_id, als_score) ---
als = (
    spark.read.format("mongo")
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", MONGO_COLLECTION_USER_EVENTS)
        .load()
        .select("user_id", "episode_id", "als_score")
)

# --- 2) Read content neighbors (episode_id, similar_episode_id, similarity) ---
sim = (
    spark.read.format("mongo")
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", MONGO_COLLECTION)
        .load()
        .select("episode_id", "similar_episode_id", "similarity")
)

# --- 3) Blend: α·ALS + (1-α)·similarity ---
alpha = float(os.getenv("HYBRID_ALPHA", "0.7"))

hybrid_raw = (
    als.alias("a")
       .join(sim.alias("s"), F.col("a.episode_id") == F.col("s.episode_id"), "inner")
       .where(F.col("a.episode_id") != F.col("s.similar_episode_id"))  # avoid self-recs
       .select(
           F.col("a.user_id").alias("user_id"),
           F.col("s.similar_episode_id").alias("recommended_episode_id"),
           (F.col("a.als_score") * F.lit(alpha) +
            F.col("s.similarity") * F.lit(1.0 - alpha)).alias("hybrid_score")
       )
)

# --- 4) Dedup & Top-N per user ---
hybrid = (
    hybrid_raw
    .groupBy("user_id", "recommended_episode_id")
    .agg(F.max("hybrid_score").alias("hybrid_score"))
)

w = Window.partitionBy("user_id").orderBy(F.desc("hybrid_score"))
final_recs = (
    hybrid
    .withColumn("rank", F.row_number().over(w))
    .filter(F.col("rank") <= TOP_N)
    .select("user_id", "recommended_episode_id", "hybrid_score")
)

# --- 5) Write to Mongo (legacy connector → "mongo") ---
(
    final_recs.write
    .format("mongo")
    .mode(os.getenv("FINAL_RECS_WRITE_MODE", "overwrite"))  # or "append"
    .option("uri", MONGO_URI)
    .option("database", MONGO_DB)
    .option("collection", MONGO_COLLECTION_FINAL_RECS)
    .save()
)

spark.stop()
