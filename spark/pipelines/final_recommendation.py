# spark/pipelines/final_recommendation.py
# Minimal hybrid recommender (ALS × content) using legacy Mongo connector ("mongodb")

import os, sys
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

from spark.util.common import get_spark

from config.settings import (
    N_FINAL_RECOMMENDATION,
    MONGO_URI, MONGO_DB,
    MONGO_COLLECTION_USER_EVENTS,  # expects (user_id, episode_id, als_score) or pre-agg 
    MONGO_COLLECTION_SIMILARITIES, # similarities: new_episode_id, historical_episode_id, similarity               
    MONGO_COLLECTION_FINAL_RECS,  # snapshot target
)

HYBRID_ALPHA = 0.7


# --- 1) ALS seeds (user_id, episode_id, als_score) ---
def read_als(spark: SparkSession):
    return (
        spark.read.format("mongodb")
            .option("spark.mongodb.read.connection.uri", MONGO_URI)
            .option("database", MONGO_DB)
            .option("collection", MONGO_COLLECTION_USER_EVENTS)
            .load()
            .select("user_id", "episode_id", "als_score")
            .withColumn("als_score", F.col("als_score").cast(T.DoubleType()))
            .dropna(subset=["user_id", "episode_id", "als_score"])
    )

# --- 2) Content neighbors (episode_id, similar_episode_id, similarity) ---
# Normalize names here so the rest of the pipeline is simple.
def read_sim(spark: SparkSession):
    return (
        spark.read.format("mongodb")
            .option("spark.mongodb.read.connection.uri", MONGO_URI)
            .option("database", MONGO_DB)
            .option("collection", MONGO_COLLECTION_SIMILARITIES)
            .load()
            .select(
                F.col("new_episode_id").alias("episode_id"),
                F.col("historical_episode_id").alias("similar_episode_id"),
                F.col("similarity").cast(T.DoubleType()).alias("similarity"),
            )
            .dropna(subset=["episode_id", "similar_episode_id", "similarity"])
    )


# --- 3) Blend: α·ALS + (1-α)·similarity ---
def compute_hybrid(als_df, sim_df, alpha: float):
    hybrid_raw = (
        als_df.alias("a")
        .join(sim_df.alias("s"), F.col("a.episode_id") == F.col("s.episode_id"), "inner")
        .where(F.col("a.episode_id") != F.col("s.similar_episode_id"))  # avoid self recs
        .select(
            F.col("a.user_id").alias("user_id"),
            F.col("s.similar_episode_id").alias("recommended_episode_id"),
            (F.col("a.als_score") * F.lit(alpha) + F.col("s.similarity") * F.lit(1.0 - alpha)).alias("score"),
        )
    )

    hybrid = (
        hybrid_raw
        .groupBy("user_id", "recommended_episode_id")
        .agg(F.max("score").alias("score"))
    )

    # remove items the user already consumed 
    user_history = (
        als_df
        .select("user_id", F.col("episode_id").alias("historical_episode_id"))
        .distinct()
    )

    hybrid_clean = hybrid.join(
        user_history,
        on=[ "user_id", hybrid.recommended_episode_id == user_history.historical_episode_id ],
        how="left_anti",
    )

    return hybrid_clean


def top_n_per_user(hybrid_df, top_n: int):
    w = Window.partitionBy("user_id").orderBy(F.desc("score"))
    return (
        hybrid_df
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= F.lit(top_n))
        .drop("rank")
        .withColumn("generated_at", F.current_timestamp())
        .withColumn("trigger_reason", F.lit("scheduled_batch"))  # optional metadata...
    )

# ---------- Write ----------
def write_snapshot(df, collection: str):
    #replace whole collection each run.
    (
        df.write
        .format("mongodb")
        .mode("overwrite")
        .option("spark.mongodb.write.connection.uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", collection)
        .save()
    )

# ---------- Main ----------
def main() -> int:
    spark = get_spark('final-recommendation')
    try:
        als = read_als(spark)
        sim = read_sim(spark)

        hybrid = compute_hybrid(als, sim, HYBRID_ALPHA)
        final_recs = top_n_per_user(hybrid, N_FINAL_RECOMMENDATION)

        # Keep only the fields that dashboard expects
        final_recs_out = final_recs.select("user_id", "recommended_episode_id", "score", "generated_at")

        write_snapshot(final_recs_out, MONGO_COLLECTION_FINAL_RECS)
        print(f"[OK] Wrote snapshot to {MONGO_DB}.{MONGO_COLLECTION_FINAL_RECS} (alpha={HYBRID_ALPHA}, topN={N_FINAL_RECOMMENDATION})")
        return 0
    except Exception as e:
        print(f"[ERROR] final_recommendation failed: {e}", file=sys.stderr)
        return 1
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
