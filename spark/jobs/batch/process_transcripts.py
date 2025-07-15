from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, Normalizer, BucketedRandomProjectionLSH
from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window
from pyspark.errors import AnalysisException
import datetime
from pyspark.ml.feature import PCA
from pyspark import StorageLevel

# Start Spark session
spark = SparkSession.builder.appName("TranscriptsBatch").getOrCreate()

# --- CONFIG ---
CURRENT_DATE = datetime.date.today().isoformat()
TOP_K = 3
DELTA_LAKE_PATH = "/data_lake/transcripts_en"
HISTORICAL_VECTORS_PATH = "/data_lake/tfidf_vectors"
SIMILARITY_PATH = "/data_lake/similarities"  # Define the similarity output path

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

    # PCA for dimensionality reduction
    pca = PCA(k=100, inputCol="features", outputCol="reduced_features")
    pca_model = pca.fit(tfidf_df)
    reduced_df = pca_model.transform(tfidf_df)

    return reduced_df.select("episode_id", "reduced_features")

#====== LSH MODEL ======
def fit_lsh_model(tfidf_df):
    lsh = BucketedRandomProjectionLSH(
        inputCol="reduced_features",  # Use reduced features after PCA
        outputCol="hashes",
        bucketLength=1.0,
        numHashTables=5
    )
    model = lsh.fit(tfidf_df)  # Fit the LSH model only once
    return model

# ==== FIND KNN ======
def find_knn(lsh_model, new_batch_df, history_df, top_k=TOP_K):
    joined = lsh_model.approxSimilarityJoin(
        new_batch_df.select("episode_id", "reduced_features"),
        history_df.select("episode_id", "reduced_features"),
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
    # Use cache to avoid recomputing tfidf in case of repeated operations
    new_batch = compute_tfidf_embeddings(DELTA_LAKE_PATH).cache()

    try:
        history = spark.read.format("delta").load(HISTORICAL_VECTORS_PATH).cache()
    except AnalysisException:
        print("No history found — starting fresh.")
        history = spark.createDataFrame([], new_batch.schema)

    # Check if there is new data and history
    if not new_batch.isEmpty() and not history.isEmpty():
        # Fit the LSH model using historical vectors
        lsh_model = fit_lsh_model(history)
        
        # Find KNN similarities
        knn_df = find_knn(lsh_model, new_batch, history) \
                    .withColumn("date", lit(CURRENT_DATE))

        # Save KNN similarities to Delta Lake
        knn_df.write.format("delta") \
            .mode("append") \
            .partitionBy("date") \
            .save("/data_lake/knn_similarities")
        
        # Optionally, store the similarities in a different path
        knn_df.write.format("delta").save(SIMILARITY_PATH)

        # Update historical vector set
        updated = history.union(new_batch).dropDuplicates(["episode_id"])
        updated.write.format("delta").mode("overwrite").save(HISTORICAL_VECTORS_PATH)

    else:
        print("Skipping similarity search — no data.")
