from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
import time

spark = SparkSession.builder.appName("ALSTraining").getOrCreate()

# === Paths === probably solved by pipeline script?
#ENGAGEMENT_AGG_PATH = "/tmp/engagement_aggregates"
#ALS_MODEL_PATH = "/models/als_model"


def train_als_model():
    als_input = spark.read.format("delta").load(ENGAGEMENT_AGG_PATH) 

    #ALS is part of Mllib, so we just implement that => a matrix factorization algorithm
    als = ALS(
        userCol="user_id",
        itemCol="podcast_id",
        ratingCol="engagement_score", #target variable
        nonnegative=True, #Enforce nonnegative factors for interpretability and stability
        coldStartStrategy="drop", #avoid NaNs in predictions for unseen users/items
        maxIter=10, #for convergence balance between speed and accuracy
        regParam=0.1 #Regularize with regParam=0.1 to avoid overfitting to sparse data
    )

    model = als.fit(als_input)
    model.write().overwrite().save(ALS_MODEL_PATH)

    print("ALS model trained and saved.")
    return model

if __name__ == "__main__":
    #adapt to streaming of user_events
    while True:
        print("Starting ALS training...")
        train_als_model()
        print("Waiting 1 hour for next retraining cycle...")
        time.sleep(3600)
