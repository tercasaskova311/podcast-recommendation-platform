from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, Normalizer, BucketedRandomProjectionLSH
from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window
from pyspark.errors import AnalysisException
import datetime
from pyspark.ml.feature import PCA
from pyspark import StorageLevel
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType
import os

spark = SparkSession.builder.appName("TranscriptsBatch").getOrCreate()

# Kafka Config
CURRENT_DATE = datetime.date.today().isoformat()
TOP_K = 3
INPUT_DELTA_LAKE = "/data_lake/transcripts_en"
VECTORS_DELTA_LAKE = "/data_lake/tfidf_vectors"
SIMILARITIES_DELTA_LAKE = "/data_lake/similarities"  # Define the similarity output path


#==== TEXT PROCESSING: TF-IDF Embeddings ====
def compute_tfidf_embeddings(delta_path):
    transcripts_df = spark.read.format("delta").load(INPUT_DELTA_LAKE) \
        .filter(col("date") == CURRENT_DATE) \
        .select("episode_id", "transcript")

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
    new_batch = compute_tfidf_embeddings(INPUT_DELTA_LAKE).cache()

    try:
        history = spark.read.format("delta").load(VECTORS_DELTA_LAKE).cache()
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
        
        knn_df = knn_df.dropDuplicates(["new_podcast", "historical_podcast"])

        # Save KNN similarities to Delta Lake
        knn_df.write.format("delta") \
            .mode("append") \
            .partitionBy("date") \
            .save("/data_lake/knn_similarities")
        
        # Optionally, store the similarities in a different path
        knn_df.write.format("delta").save(SIMILARITIES_DELTA_LAKE)

        # Update historical vector set
        updated = history.union(new_batch).dropDuplicates(["episode_id"])
        updated.write.format("delta").mode("overwrite").save(VECTORS_DELTA_LAKE)

    else:
        print("Skipping similarity search — no data.")
