from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, Normalizer, BucketedRandomProjectionLSH
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import datetime

# Start Spark session
spark = SparkSession.builder.appName("TranscriptsBatch").getOrCreate()

# --- CONFIG ---
CURRENT_DATE = datetime.date.today().isoformat()
TOP_K = 3 # idk how many podcast recommendation we want to get - we can do less/more...
DELTA_LAKE_PATH = "/data_lake/transcripts_en"
HISTORICAL_VECTORS_PATH = "/data_lake/historical_tfidf_vectors"

#==== TEXT PROCESSING:  TF-IDF Embeddings ==== 
def compute_tfidf_embeddings(delta_path, current_date):
    transcripts_df = spark.read.format("delta").load(delta_path)
    tokenizer = Tokenizer(inputCol="transcript", outputCol="words")
    words_df = tokenizer.transform(transcripts_df)

    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=100)
    tf_df = hashingTF.transform(words_df)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idf_model = idf.fit(tf_df)
    tfidf_df = idf_model.transform(tf_df)

    norm = Normalizer(inputCol="features", outputCol="normFeatures")
    norm_tfidf = norm.transform(tfidf_df)

    return norm_tfidf.select("episode_id", "normFeatures")

#====== LSH MODEL (locality sensitive hashing) to compare podcast similarity ====== 
def fit_lsh_model(tfidf_df):
    lsh = BucketedRandomProjectionLSH(
        inputCol="normFeatures",
        outputCol="hashes",
        bucketLength=1.0, #lenght of scalars for buckets
        numHashTables=3 #3 random projection - 3 chances to catch similar vectors in the same bucket
    )
    return lsh.fit(tfidf_df)

# ==== KNN =========
def find_knn(lsh_model, new_batch_df, history_df, top_k=TOP_K):
    joined = lsh_model.approxSimilarityJoin( #which vector from new_batch_df fall into the same hash buckets
        new_batch_df.select("podcast_id", "normFeatures"),
        history_df.select("podcast_id", "normFeatures"),
        threshold=float("inf")
    )

    result = joined.select(
        col("datasetA.podcast_id").alias("new_podcast"),
        col("datasetB.podcast_id").alias("historical_podcast"),
        col("distCol").alias("distance") #euclidian distance between two podcasts
    )

    w = Window.partitionBy("new_podcast").orderBy("distance") #order the distances
    return result.withColumn("rank", row_number().over(w)).filter(col("rank") <= top_k).drop("rank") #choose top k closest podcasts

# ====== Run Batch =========
if __name__ == "__main__":
    new_batch = compute_tfidf_embeddings(DELTA_LAKE_PATH, CURRENT_DATE).cache()

    try:
        history = spark.read.format("delta").load(HISTORICAL_VECTORS_PATH).cache()
    except:
        history = spark.createDataFrame([], new_batch.schema)

    if new_batch.count() > 0 and history.count() > 0:
        lsh_model = fit_lsh_model(history)
        knn_df = find_knn(lsh_model, new_batch, history)
        knn_df.write.format("delta").mode("overwrite").save(f"/data_lake/knn_similarities/day_{CURRENT_DATE}")
    else:
        print("Skipping similarity search — no data.")

    updated = history.union(new_batch).dropDuplicates(["episode_id"])
    updated.write.format("delta").mode("overwrite").save(HISTORICAL_VECTORS_PATH)
