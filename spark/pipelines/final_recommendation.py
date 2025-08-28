# spark/pipelines/final_recommendation.py
# Minimal hybrid recommender (ALS × content) using legacy Mongo connector ("mongodb")

import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

from spark.util.common import get_spark

from config.settings import (
    N_FINAL_RECOMMENDATION,
    MONGO_URI, MONGO_DB,
    MONGO_COLLECTION_USER_EVENTS,   
    MONGO_COLLECTION_SIMILARITIES,               
    MONGO_COLLECTION_FINAL_RECS, 
)

spark = get_spark('final-recommendation')

# --- 1) ALS seeds (user_id, episode_id, als_score) ---
als = (
    spark.read.format("mongodb")
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", MONGO_COLLECTION_USER_EVENTS)
        .load()
        .select("user_id", "episode_id", "als_score")
)

# --- 2) Content neighbors (episode_id, similar_episode_id, similarity) ---
# Normalize names here so the rest of the pipeline is simple.
sim = (
    spark.read.format("mongodb")
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", MONGO_COLLECTION_SIMILARITIES)
        .load()
        .select(
            F.col("new_episode_id").alias("episode_id"),
            F.col("historical_episode_id").alias("similar_episode_id"),
            F.col("similarity").cast("double").alias("similarity")
        )
)

alpha = float(os.getenv("HYBRID_ALPHA", "0.7"))

# --- 3) Blend: α·ALS + (1-α)·similarity ---
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
    .filter(F.col("rank") <= N_FINAL_RECOMMENDATION)
    .select("user_id", "recommended_episode_id", "hybrid_score")
)

# --- 5) Write to Mongo ---
(
    final_recs.write
    .format("mongodb")
    .mode("overwrite")  # or "append"
    .option("uri", MONGO_URI)
    .option("database", MONGO_DB)
    .option("collection", MONGO_COLLECTION_FINAL_RECS)
    .save()
)
