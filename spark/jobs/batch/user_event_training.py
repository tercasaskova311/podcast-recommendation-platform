from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
import time
import logging

spark = SparkSession.builder \
    .appName("ALSTraining") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/rec_engine.als_recommendations") \
    .getOrCreate()

# === Paths === 
ENGAGEMENT_AGG_PATH = "/tmp/engagement_aggregates"  # Or use MongoDB path
ALS_MODEL_PATH = "/models/als_model"

# Set up logging for better debugging
logging.basicConfig(level=logging.INFO)

def train_als_model():
    logging.info("Loading engagement aggregates for ALS training.")
    
    # Load aggregated engagement scores from MongoDB 
    als_input = spark.read.format("mongo").load(ENGAGEMENT_AGG_PATH)

    # ALS model configuration with optimized hyperparameters
    als = ALS(
        userCol="user_id",
        itemCol="episode_id",
        ratingCol="engagement_score",  # target variable
        nonnegative=True,  # Enforce nonnegative factors for interpretability
        coldStartStrategy="drop",  # Avoid NaNs in predictions for unseen users/items
        maxIter=15,  # Increased iterations for better convergence
        regParam=0.05,  # Regularization parameter to avoid overfitting
        rank=10  # Reduced rank for faster training (fine-tune based on data)
    )

    model = als.fit(als_input)
    model.write().overwrite().save(ALS_MODEL_PATH)

    logging.info(f"ALS model trained and saved to {ALS_MODEL_PATH}.")

    return model

def save_top_n_to_mongo(model, top_n=10):
    logging.info("Generating top-N recommendations...")

    user_recs = model.recommendForAllUsers(top_n)

    flattened = user_recs.select(
        col("user_id"),
        explode("recommendations").alias("rec")
    ).select(
        col("user_id"),
        col("rec.episode_id"),
        col("rec.rating").alias("als_score")
    )

    # Save to MongoDB
    flattened.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("database", "rec_engine") \
        .option("collection", "als_recommendations") \
        .save()

    logging.info("Top-N ALS recommendations saved to MongoDB.")


if __name__ == "__main__":
    while True:
        train_als_model()
        logging.info("Training ALS model completed. Sleeping for 1 hour.")
        time.sleep(3600)  # Re-train every hour
