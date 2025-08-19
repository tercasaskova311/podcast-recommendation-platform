# file: batch_knn_option_a.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.errors import AnalysisException
import numpy as np
import datetime
import os

# ---------------- CONFIG ----------------
INPUT_DELTA_LAKE        = "/data_lake/transcripts_en"      # columns: episode_id, date, transcript, ...
VECTORS_DELTA_LAKE      = "/data_lake/episode_vectors"     # columns: episode_id, model, embedding (array<float>), created_at
SIMILARITIES_DELTA_LAKE = "/data_lake/similarities"        # columns: date, new_episode_id, historical_episode_id, distance, k, model
TODAY                   = os.environ.get("BATCH_DATE", datetime.date.today().isoformat())
MODEL_NAME              = "sentence-transformers/all-MiniLM-L6-v2"   # 384-d
TOP_K                   = 3
MAX_TOKENS              = 256
OVERLAP                 = 32

spark = (SparkSession.builder
    .appName("Podcast_ExactKNN_OptionA")
    .config("spark.mongodb.write.connection.uri", os.environ.get("MONGO_URI"))
    .getOrCreate())

def df_is_empty(df):
    try:
        return df.isEmpty()
    except AttributeError:
        return df.limit(1).count() == 0

# --------- 1) LOAD TODAY'S TRANSCRIPTS ---------
trans_df = (
    spark.read.format("delta").load(INPUT_DELTA_LAKE)
         .where(col("date") == TODAY)
         .select("episode_id", "transcript")
)

if df_is_empty(trans_df):
    spark.stop()
    raise SystemExit(0)

# Collect rows to driver for embedding 
pdf = trans_df.toPandas()  # pulls all rows (already filtered by date)
episode_ids = pdf["episode_id"].astype(str).tolist()
texts = pdf["transcript"].fillna("").tolist()

# --------- 2) EMBED ON DRIVER (chunked to avoid truncation) ---------
from sentence_transformers import SentenceTransformer

def chunk_text_by_tokens(text, tokenizer, max_tokens=256, overlap=32):
    # Tokenize once (no truncation)
    t = tokenizer(
        text,
        add_special_tokens=False,
        return_attention_mask=False,
        return_token_type_ids=False,
        truncation=False
    )
    ids = t["input_ids"]
    if not ids:
        return []
    stride = max_tokens - overlap
    chunks = []
    for start in range(0, len(ids), stride):
        end = start + max_tokens
        chunk_ids = ids[start:end]
        chunk_text = tokenizer.decode(chunk_ids, skip_special_tokens=True)
        if chunk_text.strip():
            chunks.append(chunk_text)
        if end >= len(ids):
            break
    return chunks

def embed_long_document(text, model, max_tokens=256, overlap=32):
    chunks = chunk_text_by_tokens(text, model.tokenizer, max_tokens, overlap)
    if not chunks:
        return None
    # encode returns L2-normalized embeddings when normalize_embeddings=True
    embs = model.encode(chunks, normalize_embeddings=True)
    vec = np.mean(embs, axis=0)
    # re-normalize after pooling
    vec = vec / (np.linalg.norm(vec) + 1e-8)
    return vec.astype("float32")

model = SentenceTransformer(MODEL_NAME, device="cuda")  # or "cpu"

episode_vecs = []
for t in texts:
    v = embed_long_document(t, model, max_tokens=MAX_TOKENS, overlap=OVERLAP)
    if v is None:
        v = np.zeros(model.get_sentence_embedding_dimension(), dtype="float32")
    episode_vecs.append(v)

# Build Spark DF for new batch vectors (as array<float>)
new_rows = [(eid, episode_vecs[i].tolist(), MODEL_NAME, TODAY) for i, eid in enumerate(episode_ids)]
new_vec_df = spark.createDataFrame(
    new_rows,
    schema="episode_id string, embedding array<float>, model string, created_at string"
).cache()

# --------- 3) LOAD HISTORICAL VECTORS (same model) ---------
try:
    hist_df = (
        spark.read.format("delta").load(VECTORS_DELTA_LAKE)
             .where(col("model") == MODEL_NAME)
             .select("episode_id", "embedding")
    )
except AnalysisException:
    hist_df = spark.createDataFrame([], "episode_id string, embedding array<float>")

if df_is_empty(hist_df):
    # First run: nothing to compare with — just store new vectors and exit
    (new_vec_df.write.format("delta").mode("append").save(VECTORS_DELTA_LAKE))
    print(f"[INFO] History empty. Stored {new_vec_df.count()} new vectors. Exiting.")
    spark.stop()
    raise SystemExit(0)

# --------- 4) BROADCAST HISTORY ---------
hist_pd = hist_df.toPandas()
H_ids = hist_pd["episode_id"].astype(str).to_numpy()
H = np.vstack(hist_pd["embedding"].to_numpy()).astype("float32")

# Ensure normalized (cosine sim assumes L2 norm = 1)
H_norm = np.linalg.norm(H, axis=1, keepdims=True)
H_norm[H_norm == 0] = 1.0
H = H / H_norm

bc_H_ids = spark.sparkContext.broadcast(H_ids)
bc_H = spark.sparkContext.broadcast(H)

# New batch embeddings as Spark DF
new_arr_df = new_vec_df.select("episode_id", "embedding")

# --------- 5) Exact KNN per partition (cosine distance = 1 - dot) ---------
def topk_partition(rows):
    import numpy as np
    H = bc_H.value
    H_ids = bc_H_ids.value
    for r in rows:
        qid = r["episode_id"]
        x = np.array(r["embedding"], dtype=np.float32)
        n = np.linalg.norm(x)
        if n == 0:
            continue
        x /= n
        sims = H.dot(x)                 # cosine similarity
        k = min(TOP_K, sims.shape[0])
        if k == 0:
            continue
        idx = np.argpartition(-sims, k-1)[:k]
        idx = idx[np.argsort(-sims[idx])]
        for j in idx:
            distance = float(1.0 - sims[j])   # cosine distance
            yield (qid, str(H_ids[j]), distance, TOP_K)

schema = "new_episode_id string, historical_episode_id string, distance double, k int"
pairs_df = spark.createDataFrame(new_arr_df.rdd.mapPartitions(topk_partition), schema=schema)

# --------- 6) WRITE OUTPUTS TO MONGO---------
out_df = (pairs_df
          .withColumn("date", lit(TODAY))
          .withColumn("model", lit(MODEL_NAME))
          .dropDuplicates(["new_episode_id", "historical_episode_id"]))

(out_df.write
  .format("mongodb")
  .mode("append")
  .option("database", os.environ.get("MONGO_DB", "podcasts"))
  .option("collection", os.environ.get("MONGO_COLLECTION", "similarities"))
  .save())

spark.stop()
