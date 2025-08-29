from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml.recommendation import ALS #ALS => alternating least squares = collaborative filtering
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from delta.tables import DeltaTable

from spark.util.common import get_spark

from config.settings import (
    DELTA_PATH_DAILY,
    ALS_MODEL_PATH, N_RECOMMENDATION_FOR_USER,
    MONGO_URI,MONGO_DB, MONGO_COLLECTION_USER_EVENTS,
)

ALS_RANK      = 32
ALS_MAX_ITER  = 10
ALS_REG       = 0.08
ALS_ALPHA     = 40.0
MIN_ENGAGEMENT= 1e-6

spark = get_spark("user-events-training")

# -------- 1) Load daily engagement per user per episode ----------
#contains: user_id, new_episode_id, day, engagemnt, num_events, lst_ts, ...
delta = spark.read.format("delta").load(DELTA_PATH_DAILY)

#why grouping again: delta stores daily engagemnt => ALD needs one row per user = we sum across days
# rating: sum(engagemnt) per user_id + episoded_id
ratings = (
    delta.groupBy("user_id","episode_id")
    .agg(F.sum("engagement").alias("engagement"))
    .filter(F.col("engagement") > MIN_ENGAGEMENT) #removes weak signals
)

# -------- 2) Index string ids -> integer ids for ALS --------------------
#ALS cannot handle strings IDs = convert to idx
user_indexer   = StringIndexer(inputCol="user_id",   outputCol="user_idx", handleInvalid="skip")
item_indexer   = StringIndexer(inputCol="episode_id",outputCol="item_idx", handleInvalid="skip")

pipeline = Pipeline(stages=[user_indexer, item_indexer])
fitted  = pipeline.fit(ratings)

# index + keep original ids
indexed = (
    fitted.transform(ratings)
    .select("user_id", "episode_id", "user_idx", "item_idx", "engagement")
)

# build maps BEFORE dropping ids
users_map = indexed.select("user_id", "user_idx").dropDuplicates()
items_map = indexed.select("episode_id", "item_idx").dropDuplicates()

# cast/clean for ALS
data = (
    indexed
    .select(
        F.col("user_idx").cast("int").alias("user_idx"),
        F.col("item_idx").cast("int").alias("item_idx"),
        F.col("engagement").cast("float").alias("engagement"),
    )
    .dropna(subset=["user_idx", "item_idx", "engagement"])
    .filter(F.col("engagement") >= 0)
    .filter(F.col("user_idx") >= 0)
    .filter(F.col("item_idx") >= 0)
)

if data.rdd.isEmpty():
    raise RuntimeError("No training data after filtering/casting.")

# (optional) checkpoint + conservative params if you had crashes before
spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")

# -------- 3) Train ALS (implicit feedback) ------------------------------
als = ALS(
    userCol="user_idx",
    itemCol="item_idx",
    ratingCol="engagement",
    rank=ALS_RANK,
    maxIter=ALS_MAX_ITER,
    regParam=ALS_REG,
    implicitPrefs=True,
    alpha=ALS_ALPHA,
    nonnegative=True,
    numUserBlocks=4,
    numItemBlocks=4,
    checkpointInterval=2,
    coldStartStrategy="drop",
)

model = als.fit(data)

#overwriting Spark ML artifacts (model + indexers)
model.write().overwrite().save(ALS_MODEL_PATH)
fitted.write().overwrite().save(ALS_MODEL_PATH + "_indexers")

# decode recs using the maps you kept
raw_user_recs = model.recommendForAllUsers(N_RECOMMENDATION_FOR_USER)

user_recs = (
    raw_user_recs
    .withColumn("rec", F.explode("recommendations"))
    .select(
        F.col("user_idx"),
        F.col("rec.item_idx").alias("item_idx"),
        F.col("rec.rating").alias("als_score"),
    )
    .join(users_map, "user_idx")
    .join(items_map, "item_idx")
    .select("user_id", "episode_id", "als_score")
)

# Save to Mongo 
(user_recs.write
    .format("mongodb")
    .mode("overwrite")
    .option("spark.mongodb.write.connection.uri", MONGO_URI)
    .option("database", MONGO_DB)
    .option("collection", MONGO_COLLECTION_USER_EVENTS)
    .save()
)
print("Save to mongo")