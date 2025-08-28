# spark/pipelines/final_recommendation.py
# Final recommendations (ALS × content) → Mongo snapshot

import os, sys
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

from config.settings import (
    TOP_N, MONGO_URI, MONGO_DB,
    MONGO_COLLECTION_USER_EVENTS,   # expects (user_id, episode_id, als_score) or pre-agg
    MONGO_COLLECTION,               # similarities: new_episode_id, historical_episode_id, similarity
    MONGO_COLLECTION_FINAL_RECS,    # snapshot target
)

HYBRID_ALPHA = float(os.getenv("HYBRID_ALPHA", "0.7"))

# ---------- Spark ----------
def build_spark(app_name: str = "Final-Recommendations") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.mongodb.input.uri", MONGO_URI)
        .config("spark.mongodb.output.uri", MONGO_URI)
        .config("spark.dynamicAllocation.enabled", "true")
        .getOrCreate()
    )

def read_als(spark: SparkSession):
    return (
        spark.read.format("mongo")
            .option("uri", MONGO_URI)
            .option("database", MONGO_DB)
            .option("collection", MONGO_COLLECTION_USER_EVENTS)
            .load()
            .select("user_id", "episode_id", "als_score")
            .withColumn("als_score", F.col("als_score").cast(T.DoubleType()))
            .dropna(subset=["user_id", "episode_id", "als_score"])
    )

def read_sim(spark: SparkSession):
    return (
        spark.read.format("mongo")
            .option("uri", MONGO_URI)
            .option("database", MONGO_DB)
            .option("collection", MONGO_COLLECTION)
            .load()
            .select(
                F.col("new_episode_id").alias("episode_id"),
                F.col("historical_episode_id").alias("similar_episode_id"),
                F.col("similarity").cast(T.DoubleType()).alias("similarity"),
            )
            .dropna(subset=["episode_id", "similar_episode_id", "similarity"])
    )

# ---------- Compute ----------
def compute_hybrid(als_df, sim_df, alpha: float):
    # join on original episode, score candidate similar episodes
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
    # collapse duplicates (keep best score)
    hybrid = (
        hybrid_raw
        .groupBy("user_id", "recommended_episode_id")
        .agg(F.max("score").alias("score"))
    )
    return hybrid

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
        .format("mongo")
        .mode("overwrite")
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", collection)
        .save()
    )

# ---------- Main ----------
def main() -> int:
    spark = build_spark()
    try:
        als = read_als(spark)
        sim = read_sim(spark)

        hybrid = compute_hybrid(als, sim, HYBRID_ALPHA)
        final_recs = top_n_per_user(hybrid, TOP_N)

        # Keep only the fields that dashboard expects
        final_recs_out = final_recs.select("user_id", "recommended_episode_id", "score", "generated_at")

        write_snapshot(final_recs_out, MONGO_COLLECTION_FINAL_RECS)
        print(f"[OK] Wrote snapshot to {MONGO_DB}.{MONGO_COLLECTION_FINAL_RECS} (alpha={HYBRID_ALPHA}, topN={TOP_N})")
        return 0
    except Exception as e:
        print(f"[ERROR] final_recommendation failed: {e}", file=sys.stderr)
        return 1
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
