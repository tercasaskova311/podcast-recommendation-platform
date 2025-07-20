from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.ml.recommendation import ALS
import time
import logging

# === SPARK INIT ===
spark = SparkSession.builder \
    .appName("ALSTraining") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/engagement_score.user_events") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/als_recommendations.user_events") \
    .getOrCreate()

als_input = spark.read.format("mongo").load()

# === Paths === 
ALS_MODEL_PATH = "/models/als_model"
ALS_DELTA_PATH = "/data_lake/als_predictions"  # for optional Delta output

# === Logging ===
logging.basicConfig(level=logging.INFO)

def train_als_model():
    logging.info("Loading engagement scores from MongoDB...")

    als_input = spark.read.format("mongo").load()

    als = ALS(
        userCol="user_id",
        itemCol="episode_id",
        ratingCol="engagement_score",
        nonnegative=True,
        coldStartStrategy="drop",
        maxIter=15,
        regParam=0.05,
        rank=10
    )

    model = als.fit(als_input)
    model.write().overwrite().save(ALS_MODEL_PATH)

    logging.info(f"ALS model saved at {ALS_MODEL_PATH}")
    return model

def save_recommendations(model, top_n=10):
    logging.info("Generating and saving top-N recommendations...") # recommendation by user behavior... 

    recs_df = model.recommendForAllUsers(top_n)

    exploded_df = recs_df.select(
        col("user_id"),
        explode("recommendations").alias("rec")
    ).select(
        col("user_id"),
        col("rec.episode_id"),
        col("rec.rating").alias("als_score")
    )

    # Save to MongoDB (Top-N only)
    exploded_df.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("database", "recommendations")\
        .option("collection", "als_top_n")\
        .save()

    # Save to Delta Lake (entire matrix)
    exploded_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(ALS_DELTA_PATH)

    logging.info("Recommendations saved to MongoDB and Delta.")

# === MAIN LOOP ===
if __name__ == "__main__":
    while True:
        try:
            model = train_als_model()
            save_recommendations(model)
            logging.info("Sleeping for 1 hour...\n")
            time.sleep(3600) # updating every 1 hour...
        except Exception as e:
            logging.error(f"Error: {e}")
            time.sleep(600) # every 1O min 
