from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp
from pyspark.ml.recommendation import ALSModel
import time
import datetime

# ====== CONFIG ======
ALS_MODEL_PATH = "/models/als_model"
SIMILARITY_PATH = "/data_lake/knn_similarities"  # Adjusted: delta format
DELTA_ENGAGEMENT_PATH = "/tmp/engagement_aggregates"
RECOMMENDATION_OUTPUT_PATH = "/recommendations/hybrid"
ALPHA = 0.7
TOP_K = 10
REFRESH_INTERVAL = 3600  # 1 hour

# ====== SPARK SESSION ======
spark = SparkSession.builder \
    .appName("HybridRecommendationEngine") \
    .config("spark.sql.shuffle.partitions", "auto") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017") \
    .getOrCreate()


def generate_recommendations(als_model, podcast_similarities, active_users_df):
    """
    Generate hybrid recommendations using ALS + content similarity
    Only for users found in recent engagement data
    """

    # --- ALS Top-N predictions ---
    als_preds = als_model.recommendForUserSubset(active_users_df, TOP_K) \
        .selectExpr("user_id", "explode(recommendations) as rec") \
        .select("user_id", col("rec.episode_id"), col("rec.rating").alias("als_score"))

    # --- Join with content similarity matrix ---
    hybrid = als_preds.alias("als").join(
        podcast_similarities.alias("sim"),
        col("als.episode_id") == col("sim.podcast_a"),
        "left"
    ).select(
        col("als.user_id"),
        col("sim.episode_id").alias("recommended_episode_id"),
        col("als.als_score"),
        col("sim.cosine_similarity")
    )

    # --- Final hybrid score ---
    hybrid = hybrid.withColumn(
        "final_score",
        ALPHA * col("als_score") + (1 - ALPHA) * col("cosine_similarity")
    ).withColumn("generated_at", current_timestamp())

    return hybrid.orderBy("user_id", col("final_score").desc())

# ====== MAIN LOOP ======
if __name__ == "__main__":
    while True:
        try:
            now = datetime.datetime.now()

            # === Load ALS Model ===
            als_model = ALSModel.load(ALS_MODEL_PATH)

            # === Load Podcast Similarities from Delta ===
            podcast_similarities = spark.read.format("delta").load(SIMILARITY_PATH)
            podcast_similarities = podcast_similarities.dropna(subset=["cosine_similarity"])  # Sanity

            # === Load Active Users from Engagement ===
            engagement_df = spark.read.format("delta").load(DELTA_ENGAGEMENT_PATH)
            active_users = engagement_df.select("user_id").distinct()

            # === Generate Recommendations ===
            recommendations = generate_recommendations(als_model, podcast_similarities, active_users)

            recommendations.show(20, truncate=False)

            # === Save to Delta Lake ===
            recommendations.write.mode("overwrite").format("delta").save(RECOMMENDATION_OUTPUT_PATH)

            # --- Save to MongoDB ---
            recommendations.write \
                .format("mongo") \
                .mode("overwrite") \  
                .option("database", "recommendation_system") \
                .option("collection", "hybrid_recommendations") \
                .save()


            print(f"[{now}]  Done. Sleeping for {REFRESH_INTERVAL} seconds...\n")
            time.sleep(REFRESH_INTERVAL)

        except Exception as e:
            print(f"[{datetime.datetime.now()}]  ERROR: {e}")
            print("Retrying in 10 minutes...")
            time.sleep(600)
