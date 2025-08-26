from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml.recommendation import ALS #ALS => alternating least squares = collaborative filtering
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from delta.tables import DeltaTable

from spark.config.settings import (
    DELTA_PATH_DAILY,
    ALS_MODEL_PATH,TOP_N, ALS_RANK, ALS_REG, 
    ALS_MAX_ITER, ALS_ALPHA, MIN_ENGAGEMENT, MONGO_URI,
    MONGO_DB_USER_EVENTS, MONGO_COLLECTION_USER_EVENTS,
)

spark = (
    SparkSession.builder
    .appName("User-events-training")
    .config("spark.sql.session.timeZone","UTC")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# -------- 1) Load daily engagement per user per episode ----------
#contains: user_id, new_episode_id, day, engagemnt, num_events, lst_ts, ...
delta = spark.read.format("delta").load(DELTA_PATH_DAILY)

#why grouping again: delta stores daily engagemnt => ALD needs one row per user = we sum across days
# rating: sum(engagemnt) per user_id + episoded_id
ratings = (
    delta.groupBy("user_id","new_episode_id")
    .agg(F.sum("engagement").alias("engagement"))
    .filter(F.col("engagement") > MIN_ENGAGEMENT) #removes weak signals
)

# -------- 2) Index string ids -> integer ids for ALS --------------------
#ALS cannot handle strings IDs = convert to idx
user_indexer   = StringIndexer(inputCol="user_id",   outputCol="user_idx", handleInvalid="skip")
item_indexer   = StringIndexer(inputCol="new_episode_id",outputCol="item_idx", handleInvalid="skip")

pipeline = Pipeline(stages=[user_indexer, item_indexer])
fitted  = pipeline.fit(ratings)
data    = fitted.transform(ratings)

# Save the id maps to decode recs later (important!)
users_map = (data.select("user_id","user_idx").dropDuplicates())
items_map = (data.select("new_episode_id","item_idx").dropDuplicates())

# -------- 3) Train ALS (implicit feedback) ------------------------------
als = ALS(
    userCol="user_idx",
    itemCol="item_idx",
    ratingCol="engagement",
    rank=ALS_RANK, #latent dimension per user/item 
    maxIter=ALS_MAX_ITER, #iterations to converge
    regParam=ALS_REG, #regularization to prevent overfitting
    implicitPrefs=True, 
    alpha=ALS_ALPHA, #scaling factor
    nonnegative=True,
    coldStartStrategy="drop" #drop unseen users/items - prevent NaNs
)

model = als.fit(data)

#overwriting Spark ML artifacts (model + indexers)
model.write().overwrite().save(ALS_MODEL_PATH)
fitted.write().overwrite().save(ALS_MODEL_PATH + "_indexers")


# -------- 4) Top-N per user (decode ids) --------------------------------
raw_user_recs = model.recommendForAllUsers(TOP_N)
#produce column: recommendations: [{"item_idx": 17, "rating": 4.8}, {"item_idx": 52, "rating": 4.5}, ...]
#TOP N episodes personalized fro each user
user_recs = (
    raw_user_recs
    .withColumn("rec", F.explode("recommendations"))
    .select(
        F.col("user_idx"),
        F.col("rec.item_idx").alias("item_idx"),
        F.col("rec.rating").alias("als_score")
    )
    .join(users_map, "user_idx")
    .join(items_map, "item_idx")
    .select("user_id","new_episode_id","als_score")
)

# Save to Mongo 
(user_recs.write
    .format("mongo")
    .mode("overwrite")
    .option("uri", MONGO_URI)
    .option("database", MONGO_DB_USER_EVENTS)
    .option("collection", MONGO_COLLECTION_USER_EVENTS)
    .save()
)



