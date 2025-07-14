from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, Normalizer, BucketedRandomProjectionLSH
from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window
from pyspark.errors import AnalysisException
import datetime

# Start Spark session
spark = SparkSession.builder.appName("TranscriptsBatch").getOrCreate()

# --- CONFIG ---
CURRENT_DATE = datetime.date.today().isoformat()
TOP_K = 3
DELTA_LAKE_PATH = "/data_lake/transcripts_en"
HISTORICAL_VECTORS_PATH = "/data_lake/tfidf_vectors"

#==== TEXT PROCESSING: TF-IDF Embeddings ====
def compute_tfidf_embeddings(delta_path):
    transcripts_df = spark.read.format("delta").load(delta_path).filter(col("date") == CURRENT_DATE)
    tokenizer = Tokenizer(inputCol="transcript", outputCol="words")
    words_df = tokenizer.transform(transcripts_df)

    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=100)
    tf_df = hashingTF.transform(words_df)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idf_model = idf.fit(tf_df)
    tfidf_df = idf_model.transform(tf_df)

    norm = Normalizer(inputCol="features", outputCol="normFeatures")
    norm_tfidf = norm.transform(tfidf_df)

    # Always return with consistent ID naming
    return norm_tfidf.select("episode_id", "normFeatures")

#====== LSH MODEL ======
def fit_lsh_model(tfidf_df):
    lsh = BucketedRandomProjectionLSH(
        inputCol="normFeatures",
        outputCol="hashes",
        bucketLength=1.0,
        numHashTables=3
    )
    return lsh.fit(tfidf_df)

# ==== FIND KNN ======
def find_knn(lsh_model, new_batch_df, history_df, top_k=TOP_K):
    joined = lsh_model.approxSimilarityJoin(
        new_batch_df.select("episode_id", "normFeatures"),
        history_df.select("episode_id", "normFeatures"),
        threshold=float("inf")
    )

    result = joined.select(
        col("datasetA.episode_id").alias("new_podcast"),
        col("datasetB.episode_id").alias("historical_podcast"),
        col("distCol").alias("distance")
    )

    result = result.filter(col("new_podcast") != col("historical_podcast"))

    w = Window.partitionBy("new_podcast").orderBy("distance")
    return result.withColumn("rank", row_number().over(w)) \
                 .filter(col("rank") <= top_k) \
                 .drop("rank")

# ====== RUN BATCH ======
if __name__ == "__main__":
    new_batch = compute_tfidf_embeddings(DELTA_LAKE_PATH).persist()

    try:
        history = spark.read.format("delta").load(HISTORICAL_VECTORS_PATH).cache()
    except AnalysisException:
        print("No history found — starting fresh.")
        history = spark.createDataFrame([], new_batch.schema)

    if new_batch.count() > 0 and history.count() > 0:
        lsh_model = fit_lsh_model(history)
        knn_df = find_knn(lsh_model, new_batch, history) \
                    .withColumn("date", lit(CURRENT_DATE))

        knn_df.write.format("delta") \
            .mode("append") \
            .partitionBy("date") \
            .save("/data_lake/knn_similarities")
    else:
        print("Skipping similarity search — no data.")

    # update historical vector set
    updated = history.union(new_batch).dropDuplicates(["episode_id"])
    updated.write.format("delta").mode("overwrite").save(HISTORICAL_VECTORS_PATH)
