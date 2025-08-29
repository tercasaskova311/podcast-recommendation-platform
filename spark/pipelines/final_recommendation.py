import os, sys
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

from spark.util.common import get_spark

from config.settings import (
    TOP_N, MONGO_URI, MONGO_DB,
    MONGO_COLLECTION_USER_EVENTS,   
    MONGO_COLLECTION_SIMILARITIES,               
    MONGO_COLLECTION_FINAL_RECS,    
    MONGO_COLLECTION_USER_HISTORY,  
)

HYBRID_ALPHA = 0.7

def read_history(spark: SparkSession):
    return (
        spark.read.format("mongo")
            .option("uri", MONGO_URI)
            .option("database", MONGO_DB)
            .option("collection", MONGO_COLLECTION_USER_HISTORY)
            .load()
            .select(
                F.col("user_id"),
                F.col("episode_id").alias("historical_episode_id"))
    )

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

# COMPUTE FINAL RECS
def compute_hybrid(als_df, sim_df, history_df, alpha: float):

    transcript_similarity = (
        history_df.alias("h")
        .join(
            sim_df.alias("s"),
            F.col("h.historical_episode_id") == F.col("s.similar_episode_id"),
            "inner"
        )
        .groupBy(
            F.col("h.user_id").alias("user_id"),
            F.col("s.episode_id").alias("episode_id")  
        )
        .agg(F.max("s.similarity").alias("similarity"))  
    )

    hybrid = (
        als_df.alias("a")
        .join(
            transcript_similarity.alias("c"),
            (F.col("a.user_id") == F.col("c.user_id")) &
            (F.col("a.episode_id") == F.col("c.episode_id")),
            "left"
        )
        .withColumn("similarity", F.coalesce(F.col("c.similarity"), F.lit(0.0)))
        .select(
            F.col("a.user_id").alias("user_id"),
            F.col("a.episode_id").alias("recommended_episode_id"),
            (F.col("a.als_score") * F.lit(alpha) +
             F.col("similarity") * F.lit(1.0 - alpha)).alias("score"),
        )
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
    )

# ---------- Write ----------
def write_snapshot(df, collection: str):
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
        hist = read_history(spark)

        hybrid = compute_hybrid(als, sim, hist, HYBRID_ALPHA)  # <-- pass hist
        final_recs = top_n_per_user(hybrid, TOP_N)

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


