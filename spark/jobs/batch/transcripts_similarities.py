from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, BucketedRandomProjectionLSH, PCA
from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window
from pyspark.errors import AnalysisException
import datetime
import os

# ====== CONFIG (define if not already) ======
INPUT_DELTA_LAKE = "/data_lake/transcripts_en"
VECTORS_DELTA_LAKE = "/data_lake/tfidf_vectors"
SIMILARITIES_DELTA_LAKE = "/data_lake/similarities"
CURRENT_DATE = datetime.date.today().isoformat()
TOP_K = 3 #num of top k simular podcasts to commpute

spark = SparkSession.builder.appName("Transcript_Similarity").getOrCreate()

# ====== 1. TF-IDF + PCA ======
def compute_tfidf_embeddings(delta_path):
    transcripts_df = spark.read.format("delta").load(delta_path) \
        .filter(col("date") == CURRENT_DATE) \
        .select("episode_id", "transcript")

    if transcripts_df.isEmpty():
        return None

    # Tokenization
    tokenizer = Tokenizer(inputCol="transcript", outputCol="words")
    words_df = tokenizer.transform(transcripts_df)

    # TF-IDF
    tf = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=100)
    tf_df = tf.transform(words_df)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idf_model = idf.fit(tf_df)
    tfidf_df = idf_model.transform(tf_df)

    # PCA - because LSH would be hardly computed otherwise....
    pca = PCA(k=100, inputCol="features", outputCol="reduced_features")
    pca_model = pca.fit(tfidf_df)
    reduced_df = pca_model.transform(tfidf_df)

    return reduced_df.select("episode_id", "reduced_features")

# ====== 2. Fit LSH Model ======
def fit_lsh_model(reduced_df):
    if reduced_df.isEmpty():
        return None

    lsh = BucketedRandomProjectionLSH(
        inputCol="reduced_features",
        outputCol="hashes",
        bucketLength=1.0,
        numHashTables=5
    )
    return lsh.fit(reduced_df)

# ====== 3. Approx KNN Search ======
def find_knn(lsh_model, new_batch_df, history_df, top_k=TOP_K):
    if new_batch_df.isEmpty() or history_df.isEmpty():
        return None

    joined = lsh_model.approxSimilarityJoin(
        new_batch_df.select("episode_id", "reduced_features"),
        history_df.select("episode_id", "reduced_features"),
        threshold=float("inf")
    )

    result = joined.select(
        col("datasetA.episode_id").alias("new_podcast"),
        col("datasetB.episode_id").alias("historical_podcast"),
        col("distCol").alias("distance")
    ).filter(col("new_podcast") != col("historical_podcast"))

    w = Window.partitionBy("new_podcast").orderBy("distance")
    return result.withColumn("rank", row_number().over(w)) \
                 .filter(col("rank") <= top_k) \
                 .drop("rank")


# ====== RUN BATCH JOB ======
if __name__ == "__main__":
    # 1. Compute TF-IDF + PCA for today
    new_batch = compute_tfidf_embeddings(INPUT_DELTA_LAKE)
    
    if new_batch is None or new_batch.isEmpty():
        exit(0)

    new_batch = new_batch.cache()

    # 2. Load historical vectors
    try:
        history = spark.read.format("delta").load(VECTORS_DELTA_LAKE).cache()
    except AnalysisException:
        history = spark.createDataFrame([], new_batch.schema)

    # 3. Train LSH and compute similarities
    if not history.isEmpty():
        lsh_model = fit_lsh_model(history)

        if lsh_model:
            knn_df = find_knn(lsh_model, new_batch, history)

            if knn_df is not None and not knn_df.isEmpty():
                knn_df = knn_df.withColumn("date", lit(CURRENT_DATE)) \
                               .dropDuplicates(["new_podcast", "historical_podcast"])

                # Save to Delta
                knn_df.write.format("delta") \
                    .mode("append") \
                    .partitionBy("date") \
                    .save(SIMILARITIES_DELTA_LAKE)

                print(f"[INFO] Saved {knn_df.count()} similarities to {SIMILARITIES_DELTA_LAKE}.")
            else:
                print("[INFO] No similar pairs found — skipping write.")
        else:
            print("[WARN] LSH model was not trained properly.")
    else:
        print("[INFO] History empty — skipping similarity join.")

    # 4. Update historical TF-IDF vector store
    updated = history.union(new_batch).dropDuplicates(["episode_id"])
    updated.write.format("delta").mode("overwrite").save(VECTORS_DELTA_LAKE)
    print(f"[INFO] Updated historical TF-IDF vectors at {VECTORS_DELTA_LAKE}.")
