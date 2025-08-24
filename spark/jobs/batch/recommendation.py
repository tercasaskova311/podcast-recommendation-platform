from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, current_timestamp, row_number, when, lit,
    min as smin, max as smax
)
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALSModel
import os, datetime

# ====== CONFIG ======
ALS_MODEL_PATH = "/models/als_model"

# read similarities from MongoDB
SIMILARITY_FORMAT = "mongo"       # ("mongo" or "delta")
SIMILARITY_PATH   = "/data_lake/similarities"  # only used if FORMAT == "delta"

# IMPORTANT: inside Docker/cluster, use the service hostname (e.g., "mongodb"), not "localhost"
MONGO_URI         = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB          = os.getenv("MONGO_DB", "recommendations")
ALS_COLLECTION    = os.getenv("ALS_COLLECTION", "als_top_n")
SIM_COLLECTION    = os.getenv("SIM_COLLECTION", "similarities")
HYBRID_COLLECTION = os.getenv("HYBRID_COLLECTION", "hybrid_recommendations")

DELTA_ENGAGEMENT_PATH     = "/data_lake/als_predictions"   # active users
RECOMMENDATION_OUTPUT_PATH = "/data_lake/recommendations"

ALPHA  = float(os.getenv("ALPHA", "0.7"))   # behavior weight
TOP_K  = int(os.getenv("TOP_K", "10"))

# ====== SPARK SESSION ======
spark = (
    SparkSession.builder
    .appName("HybridRecommendationEngine")
    .config("spark.sql.shuffle.partitions", "auto")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.mongodb.read.connection.uri",  MONGO_URI)
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .getOrCreate()
)

def load_similarities():
    """
    Expect Mongo docs with:
      new_episode_id: string
      historical_episode_id: string
      similarity: double  (cosine, higher is better)
      model, created_at (optional but useful)
    """
    if SIMILARITY_FORMAT == "mongo":
        # project only what we need (faster + stable schema)
        pipeline = '[{"$project":{"_id":0,"new_episode_id":1,"historical_episode_id":1,"similarity":1,"model":1,"created_at":1}}]'
        sim = (spark.read.format("mongo")
               .option("database", MONGO_DB)
               .option("collection", SIM_COLLECTION)
               .option("pipeline", pipeline)
               .load())
    else:
        sim = spark.read.format("delta").load(SIMILARITY_PATH)

    # sanity: required columns
    for need in ["new_episode_id", "historical_episode_id", "similarity"]:
        if need not in sim.columns:
            raise ValueError(f"Similarities missing required column: {need}")

    # cast types explicitly
    sim = (sim
        .withColumn("new_episode_id",        col("new_episode_id").cast("string"))
        .withColumn("historical_episode_id", col("historical_episode_id").cast("string"))
        .withColumn("similarity",            col("similarity").cast("double"))
    )

    # keep latest per (new, hist, model) if duplicates exist
    if "created_at" in sim.columns and "model" in sim.columns:
        w = Window.partitionBy("new_episode_id","historical_episode_id","model").orderBy(col("created_at").desc())
        sim = sim.withColumn("_rn", row_number().over(w)).filter(col("_rn")==1).drop("_rn")

    return sim.select("new_episode_id","historical_episode_id","similarity")

def load_als_topn():
    # expected doc: { user_id, recommendations: [{episode_id, rating}, ...] }
    als = (spark.read.format("mongo")
           .option("database", MONGO_DB)
           .option("collection", ALS_COLLECTION)
           .load())

    als = (als
        .selectExpr("user_id", "explode(recommendations) as rec")
        .select(
            col("user_id").cast("string").alias("user_id"),
            col("rec.episode_id").cast("string").alias("seed_episode_id"),
            col("rec.rating").cast("double").alias("als_score")
        ))
    return als

def load_active_users():
    # Optional gate to reduce compute to recently active users
    try:
        engagement = spark.read.format("delta").load(DELTA_ENGAGEMENT_PATH)
        return engagement.select(col("user_id").cast("string")).distinct()
    except Exception:
        return None

def normalize_als_per_user(df):
    """Min-max normalize ALS scores per user to [0,1]; constant scores → 0.5."""
    w = Window.partitionBy("user_id")
    return (df
        .withColumn("min_s", smin("als_score").over(w))
        .withColumn("max_s", smax("als_score").over(w))
        .withColumn(
            "als_score_norm",
            when(col("max_s") > col("min_s"),
                 (col("als_score") - col("min_s")) / (col("max_s") - col("min_s")))
            .otherwise(lit(0.5))
        )
        .drop("min_s","max_s")
    )

def generate_recommendations(sim_df, active_users_df):
    als = load_als_topn()

    if active_users_df is not None:
        als = als.join(active_users_df, on="user_id", how="inner")

    if als.rdd.isEmpty():
        return spark.createDataFrame(
            [], "user_id string, recommended_episode_id string, final_score double, generated_at timestamp"
        )

    als = normalize_als_per_user(als)

    # expand each ALS seed to similar items
    hybrid = (als.alias("a")
        .join(sim_df.alias("s"), col("a.seed_episode_id") == col("s.new_episode_id"), "left")
        .select(
            col("a.user_id"),
            col("a.seed_episode_id"),
            col("a.als_score_norm"),
            col("s.historical_episode_id").alias("candidate_episode_id"),
            col("s.similarity")
        )
        .filter(col("candidate_episode_id").isNotNull())
        .filter(col("candidate_episode_id") != col("seed_episode_id"))
    )

    hybrid = hybrid.withColumn(
        "final_score",
        ALPHA * col("als_score_norm") + (1 - ALPHA) * col("similarity")
    ).withColumn("generated_at", current_timestamp())

    # Top-N per user
    w = Window.partitionBy("user_id").orderBy(col("final_score").desc())
    top_n = (hybrid
        .withColumn("rank", row_number().over(w))
        .filter(col("rank") <= TOP_K)
        .select(
            col("user_id"),
            col("candidate_episode_id").alias("recommended_episode_id"),
            col("final_score"),
            col("generated_at")
        ))
    return top_n

def main():
    try:
        _ = ALSModel.load(ALS_MODEL_PATH)  # optional
    except Exception:
        pass

    sim_df = load_similarities()
    active_users = load_active_users()
    recs = generate_recommendations(sim_df, active_users)

    recs.show(20, truncate=False)

    # Delta (keep history; partition by date)
    (recs
        .withColumn("date", current_timestamp().cast("date"))
        .write
        .mode("overwrite")
        .format("delta")
        .partitionBy("date")
        .save(RECOMMENDATION_OUTPUT_PATH))

    # Mongo (append so we keep history)
    (recs
        .withColumn("source", lit("hybrid_v1"))
        .write
        .format("mongo")
        .mode("append")
        .option("database", MONGO_DB)
        .option("collection", HYBRID_COLLECTION)
        .save())

    print(f"[{datetime.datetime.utcnow().isoformat()}Z] Wrote recommendations.")

if __name__ == "__main__":
    main()
