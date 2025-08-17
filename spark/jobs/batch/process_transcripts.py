from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, Normalizer, BucketedRandomProjectionLSH
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import datetime

# Start Spark session
spark = SparkSession.builder.appName("TranscriptsBatch").getOrCreate()

# --- CONFIG ---
CURRENT_DATE = datetime.date.today().isoformat()
TOP_K = 3 # idk how many podcast recommendation we want to get - we can do less/more...

#=== PATH ===== idk how this is exactly handle through pipelines folder? 
#NEW_BATCH_PATH = f"/transcripts_data/batch_{CURRENT_DATE}/" 
#HISTORICAL_VECTORS_PATH = "/output/historical_tfidf_vectors" 

#==== TEXT PROCESSING:  TF-IDF Embeddings ==== 
def compute_tfidf_embeddings(input_path):
    transcripts_df = spark.read.option("multiline", "true").json(input_path).select("podcast_id", "transcript")
    tokenizer = Tokenizer(inputCol="transcript", outputCol="words")
    words_df = tokenizer.transform(transcripts_df)

    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=100)
    tf_df = hashingTF.transform(words_df)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idf_model = idf.fit(tf_df)
    tfidf_df = idf_model.transform(tf_df)

    norm = Normalizer(inputCol="features", outputCol="normFeatures")
    norm_tfidf = norm.transform(tfidf_df)

    return norm_tfidf.select("podcast_id", "normFeatures") #do I save normalized tfidf into data lake too? 

#====== LSH MODEL (locality sensitive hashing) to compare podcast similarity ====== 
'''
In LSH, we use random linear projections — basically dot products.
We take a high-dim vector x (TF-IDF vector), and project it onto a random direction r.
This gives us a scalar — the coordinate of x along r.
Then we split the line (i.e. the scalar space) into equal-sized intervals ( == buckets).
Two similar vectors will likely end up in the same bucket 
(i.e. their projections fall into the same interval).
We do this multiple times with different random directions (hash tables) 
to increase the chance that similar vectors collide in 
at least one bucket. This way, we only compare items 
within the same buckets — which drastically speeds up nearest neighbor search.
'''
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
    '''
    approxSimilarityJoin: This method compares all 
    vectors from datasetA (new_batch_df) to datasetB (history_df) 
    — but only within the same hash buckets, 
    which massively reduces computation.
    '''
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
    print(f"Processing new batch from: {NEW_BATCH_PATH}")
    new_batch = compute_tfidf_embeddings(NEW_BATCH_PATH)
    new_batch.cache()

    print(" Loading historical data...")

    try:
        history = spark.read.write.format("delta").save(path)
    except:
        history = spark.createDataFrame([], new_batch.schema)
    '''
    This does the following: Tries to load previously saved 
    vectors from the path /output/historical_tfidf_vectors
    If nothing exists yet (i.e. first batch), it creates an empty 
    DataFrame with the same schema as the new batch — which means 
    find_knn() will run but just not return any neighbors 
    (which is OK for the first run). So history_df is just 
    this history variable.'''
    history.cache()

    if history.count() > 0 and new_batch.count() > 0:
        lsh_model = fit_lsh_model(history)

        print(" Finding nearest neighbors...")
        knn_df = find_knn(lsh_model, new_batch, history)
        knn_df.write.mode("overwrite").parquet(f"/output/knn_similarities/day_{CURRENT_DATE}")
    else:
        print(" Skipping similarity search (no data).")

    print("Saving updated historical vectors...")
    updated_history = history.union(new_batch).dropDuplicates(["podcast_id"])
    updated_history.write.mode("overwrite").parquet(HISTORICAL_VECTORS_PATH) #now it is set up to save as parquet to I save in data lake? but idk if that is correct

    print("Dail y podcast similarity batch complete.")
