from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, row_number
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALSModel
import time
import datetime

# ====== CONFIG ======
ALS_MODEL_PATH = "/models/als_model"
SIMILARITY_PATH = "/data_lake/similarities"  
DELTA_ENGAGEMENT_PATH = "/data_lake/als_predictions"
RECOMMENDATION_OUTPUT_PATH = "/data_lake/recommendations"
ALPHA = 0.7
TOP_K = 10
REFRESH_INTERVAL = 3600  # 1 hour

# ====== SPARK SESSION ======
spark = SparkSession.builder \
    .appName("HybridRecommendationEngine") \
    .config("spark.sql.shuffle.partitions", "auto") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017") \
    .getOrCreate()

# ====== HYBRID LOGIC ======
def generate_recommendations(podcast_similarities, active_users_df):
    """
    Generate hybrid recommendations using ALS + content similarity
    Only for users found in recent engagement data
    """

    # --- Load ALS Top-N from MongoDB ---
    als_preds = spark.read.format("mongo") \
        .option("database", "recommendations") \
        .option("collection", "als_top_n") \
        .load()
    
    als_preds = als_preds.selectExpr("user_id", "explode(recommendations) as rec") \
                         .select("user_id", col("rec.episode_id"), col("rec.rating").alias("als_score"))

    # --- Filter only active users ---
    als_preds = als_preds.join(active_users_df, on="user_id", how="inner")

    # --- Join with content similarity matrix ---
    hybrid = als_preds.alias("als").join(
        podcast_similarities.alias("sim"),
        col("als.episode_id") == col("sim.podcast_a"),  # adjust if needed
        "left"
    ).select(
        col("als.user_id"),
        col("sim.episode_id").alias("recommended_episode_id"),
        col("als.als_score"),
        col("sim.cosine_similarity")
    )

    # --- Final hybrid score --- = it simply is a weighted sum - now it is set to user_ behaior (ALS) 70% a transcribts of podcasts 30% = meaning we do trust in recommendation more to user behavior....
    hybrid = hybrid.withColumn(
        "final_score",
        ALPHA * col("als_score") + (1 - ALPHA) * col("cosine_similarity")
    ).withColumn("generated_at", current_timestamp())

    # --- Top-N per user using window function ---
    w = Window.partitionBy("user_id").orderBy(col("final_score").desc())
    top_n_df = hybrid.withColumn("rank", row_number().over(w)) \
                     .filter(col("rank") <= TOP_K) \
                     .drop("rank")

    return top_n_df


# ====== MAIN LOOP ======
if __name__ == "__main__":
    while True:
        try:
            now = datetime.datetime.now()

            # === Load ALS Model (optional, if you use it for future scoring) ===
            als_model = ALSModel.load(ALS_MODEL_PATH)

            # === Load Podcast Similarities from Delta ===
            podcast_similarities = spark.read.format("delta").load(SIMILARITY_PATH)
            podcast_similarities = podcast_similarities.dropna(subset=["cosine_similarity"])

            # === Load Active Users from Engagement ===
            engagement_df = spark.read.format("delta").load(DELTA_ENGAGEMENT_PATH)
            active_users = engagement_df.select("user_id").distinct()

            # === Generate Recommendations ===
            recommendations = generate_recommendations(podcast_similarities, active_users)

            recommendations.show(20, truncate=False)

            # === Save to Delta Lake ===
            recommendations.write \
                .mode("overwrite") \
                .format("delta") \
                .save(RECOMMENDATION_OUTPUT_PATH)

            # === Save to MongoDB ===
            recommendations.write \
                .format("mongo") \
                .mode("overwrite") \
                .option("database", "recommendation_system") \
                .option("collection", "hybrid_recommendations") \
                .save()

            print(f"[{now}] Done. Sleeping for {REFRESH_INTERVAL} seconds...\n")
            time.sleep(REFRESH_INTERVAL)

        except Exception as e:
            print(f"[{datetime.datetime.now()}] ERROR: {e}")
            print("Retrying in 10 minutes...")
            time.sleep(600)
