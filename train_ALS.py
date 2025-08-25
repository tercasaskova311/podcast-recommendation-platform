from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from delta.tables import DeltaTable

from pyspark.sql import SparkSession, functions as F
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from spark.config.settings import (
    DELTA_PATH_DAILY,
    ALS_MODEL_PATH, ALS_INDEXERS_PATH,
    ALS_DELTA_PATH, ALS_ITEMITEM_PATH,
    TOP_N, ALS_RANK, ALS_REG, ALS_MAX_ITER, ALS_ALPHA, MIN_ENGAGEMENT, MONGO_URI,
)

spark = (
    SparkSession.builder
    .appName("ALS-Train-And-Score")
    .config("spark.sql.session.timeZone","UTC")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# -------- 1) Load daily engagement per user per episode ----------
delta = spark.read.format("delta").load(DELTA_PATH_DAILY)
#DELTA_PATH_DAILY   (daily aggregates: user, episode, day)

#rating: sum(engagemnt) per user_id + episoded_id
ratings = (
    delta.groupBy("user_id","new_episode_id")
    .agg(F.sum("engagement").alias("engagement"))
    .filter(F.col("engagement") > MIN_ENGAGEMENT)
)

# -------- 2) Index string ids -> integer ids for ALS --------------------
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
    rank=ALS_RANK,
    maxIter=ALS_MAX_ITER,
    regParam=ALS_REG,
    implicitPrefs=True,
    alpha=ALS_ALPHA,
    nonnegative=True,
    coldStartStrategy="drop"
)

model = als.fit(data)

# Persist model + the indexer pipeline metadata (to reuse mappings)
model.write().overwrite().save(ALS_MODEL_PATH)
# Optionally also save the StringIndexers
fitted.write().overwrite().save(ALS_MODEL_PATH + "_indexers")


# -------- 4) Top-N per user (decode ids) --------------------------------
raw_user_recs = model.recommendForAllUsers(TOP_N)
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
    .option("database", "recommendations")
    .option("collection", "als_top_n")
    .save()
)

# -------- 5) Item-to-item recs (for “because you watched …”) ------------
raw_item_recs = model.recommendForAllItems(TOP_N)
item_item = (
    raw_item_recs
    .withColumn("rec", F.explode("recommendations"))
    .select(
        F.col("item_idx").alias("episode_idx"),
        F.col("rec.item_idx").alias("similar_idx"),
        F.col("rec.rating").alias("similarity")
    )
    .join(items_map.withColumnRenamed("item_idx","new_episode_idx"), "new_episode_idx")
    .withColumnRenamed("new_episode_id","new_episode_id_src")
    .join(items_map.withColumnRenamed("item_idx","similar_idx"), "similar_idx")
    .withColumnRenamed("new_episode_id","new_episode_id_sim")
    .select("new_episode_id_src","new_episode_id_sim","similarity")
)

# Save item-item to Mongo
(item_item.write
    .format("mongo")
    .mode("overwrite")
    .option("uri", MONGO_URI)
    .option("database", "recommendations")
    .option("collection", "als_item_item")
    .save()
)

