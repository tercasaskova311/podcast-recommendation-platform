from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = (SparkSession.builder.appName("hybrid-test").getOrCreate())

als = spark.createDataFrame([
    ("u1","e1",0.92), ("u1","e2",0.74),
    ("u2","e2",0.83), ("u2","e3",0.61),
], ["user_id","episode_id","als_score"])

sim = spark.createDataFrame([
    ("e1","e4",0.95), ("e1","e5",0.85),
    ("e2","e6",0.75), ("e2","e5",0.60),
    ("e3","e7",0.65),
], ["episode_id","similar_episode_id","similarity"])

alpha = 0.7

hybrid_raw = (
    als.alias("a")
       .join(sim.alias("s"), F.col("a.episode_id")==F.col("s.episode_id"), "inner")
       .where(F.col("a.episode_id") != F.col("s.similar_episode_id"))
       .select(
           F.col("a.user_id"),
           F.col("s.similar_episode_id").alias("recommended_episode_id"),
           (F.col("a.als_score")*F.lit(alpha) + F.col("s.similarity")*F.lit(1.0-alpha)).alias("hybrid_score")
       )
)

hybrid = (hybrid_raw
    .groupBy("user_id","recommended_episode_id")
    .agg(F.max("hybrid_score").alias("hybrid_score")))

TOP_N = 2
w = Window.partitionBy("user_id").orderBy(F.desc("hybrid_score"))
final_recs = (hybrid
    .withColumn("rank", F.row_number().over(w))
    .filter(F.col("rank") <= TOP_N)
    .select("user_id","recommended_episode_id","hybrid_score"))

final_recs.show(truncate=False)
