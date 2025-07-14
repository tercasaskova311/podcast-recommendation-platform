# ======= COMPUTING RECOMMENDATION based on engagement score and simularities between podcasts ========
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.ml.recommendation import ALSModel
import time


spark = SparkSession.builder.appName("HybridRecommendation").getOrCreate()

#====== PATH ============== I think this is probably handled by pipelines right? 
ALS_MODEL_PATH = "/models/als_model" #pretrained ALS model from ALS_training.py
SIMILARITY_PATH = "/output/podcast_similarities" #precompute podcast similarities in process_transcrip.py 


#===== RECOMMENDATION COMPUTATION ============
def generate_recommendations(als_model, podcast_similarities):
    """
    Generate hybrid podcast recommendations by combining:
    - Collaborative filtering scores from ALS (personalized user preferences)
    - Content similarity scores from podcast similarity matrix (item-item similarity)
    """

    # --- ALS Predictions ---
    # 'recommendForAllUsers(10)' returns top 10 podcast recommendations per user.
    # The result has a nested structure: recommendations is an array of structs,
    # each containing (podcast_id, rating).

    als_preds = als_model.recommendForAllUsers(10) \
        .selectExpr("user_id", "explode(recommendations) as rec") \
        .select("user_id", col("rec.podcast_id"), col("rec.rating").alias("als_score"))


    # --- Join with Podcast Similarities ---
    # We perform a LEFT JOIN between ALS predictions and the podcast similarity matrix.
    # The similarity matrix stores pairs of podcasts (podcast_a, podcast_b) with their cosine similarity.
    # Joining on ALS's recommended podcast to find similar podcasts.
    # This step enriches collaborative filtering results with content-based signals.
    hybrid = als_preds.alias("als").join(
        podcast_similarities.alias("sim"),
        col("als.podcast_id") == col("sim.podcast_a"),
        "left"
    ).select(
        col("als.user_id"),
        col("sim.podcast_b").alias("recommended_podcast_id"),
        col("als.als_score"),
        col("sim.cosine_similarity")
    )

     # --- Combine Scores for Hybrid Ranking ---
    # We use a weighted average of ALS and similarity scores.
    # Alpha controls the weight of collaborative filtering vs content similarity.
    alpha = 0.7
    hybrid = hybrid.withColumn(
        "final_score",
        alpha * col("als_score") + (1 - alpha) * col("cosine_similarity")
    )

    # --- Final Ordering ---
    # Sort recommendations by user and descending combined score for ranking.
    return hybrid.orderBy("user_id", col("final_score").desc())

if __name__ == "__main__":
     # This loop continuously refreshes recommendations every hour.
    # Useful for a production system that updates model/similarities periodically.


    while True:
        print("Loading ALS model...")
        als_model = ALSModel.load(ALS_MODEL_PATH)

        print("Loading podcast similarity matrix...")
        podcast_similarities = spark.read.parquet(SIMILARITY_PATH)

        print("Generating hybrid recommendations...")
        recommendations = generate_recommendations(als_model, podcast_similarities)

        recommendations.show(20, truncate=False)

        print("Waiting 1 hour for next recommendation cycle...")
        time.sleep(3600) #updated every hour
