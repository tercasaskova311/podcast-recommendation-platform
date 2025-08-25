# analyze_transcripts_pipeline.py
# Run with:  python -m spark.pipelines.analyze_transcripts_pipeline
# Purpose:   Embed transcripts -> write vectors (Delta, idempotent) -> compute top-K similarities -> write to Mongo (or Delta fallback)

import os, datetime
from typing import Iterable, List, Optional, Sequence, Tuple
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DataType
from pyspark.sql import DataFrame, SparkSession
from pyspark.errors import AnalysisException
from delta.tables import DeltaTable
from sentence_transformers import SentenceTransformer

from spark.util.common import get_spark
from util.delta_io import ensure_table    


from config.settings import (
    DELTA_PATH_TRANSCRIPTS, DELTA_PATH_VECTORS, DELTA_PATH_SIMILARITIES,
    MONGO_URI, MONGO_DB, MONGO_COLLECTION_SIMILARITIES
)
BATCH_DATE = os.getenv("BATCH_DATE")#where does this take the value??

# Model + embedding config
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
MAX_TOKENS = 512
OVERLAP = 32
SAFETY_MARGIN = 8
TOP_K = 3
BATCH_SIZE = 64
DEVICE = "cpu"

# Control flags
RECOMPUTE_ALL = False
WITHIN_BATCH_IF_EMPTY = True

def log(msg: str, level: str = "INFO") -> None:
    print(f"[{level}] {msg}")
   

# ---------------- Embedding helpers ----------------
def chunk_text_by_tokens(
    text: str,
    tokenizer,
    max_tokens: int,
    overlap: int = OVERLAP,
    safety_margin: int = SAFETY_MARGIN
) -> List[str]:
    if not text:
        return []
    t = tokenizer(text, add_special_tokens=False, return_attention_mask=False,
                  return_token_type_ids=False, truncation=False)
    ids = t.get("input_ids", [])
    if not ids:
        return []
    num_special = tokenizer.num_special_tokens_to_add(pair=False)
    tok_ceiling = getattr(tokenizer, "model_max_length", max_tokens) or max_tokens
    eff_max = min(max_tokens, tok_ceiling)
    window = max(8, eff_max - num_special - safety_margin)
    stride = max(1, window - overlap)
    out = []
    for start in range(0, len(ids), stride):
        end = min(start + window, len(ids))
        if start >= end:
            break
        txt = tokenizer.decode(ids[start:end], skip_special_tokens=True).strip()
        if txt:
            out.append(txt)
        if end >= len(ids):
            break
    return out

def embed_long_document(
    text: str,
    model: SentenceTransformer,
    max_tokens: int,
    overlap: int,
    safety_margin: int,
    batch_size: int
):
    chunks = chunk_text_by_tokens(text, model.tokenizer, max_tokens, overlap, safety_margin)
    if not chunks:
        return None

    # Token counts per chunk (as weights)
    tok_counts = [len(model.tokenizer(c, add_special_tokens=True)["input_ids"]) for c in chunks]

    # Get chunk embeddings in batches; already L2-normalized per chunk
    embs = model.encode(chunks, batch_size=batch_size, normalize_embeddings=True)
    embs = np.asarray(embs, dtype="float32")

    # Token-length weighted mean on the hypersphere
    w = np.asarray(tok_counts, dtype="float32")
    w = w / (w.sum() + 1e-8)

    vec = (embs * w[:, None]).sum(axis=0)
    vec /= (np.linalg.norm(vec) + 1e-8)  # final renorm
    return vec.astype("float32")

# ---------------- KNN ----------------
def compute_topk_pairs(
    spark: SparkSession,
    new_vec_df: DataFrame,
    hist_df: DataFrame,
    model_dim: int,
) -> Tuple[DataFrame, bool]:

    used_within = False
    if (hist_df.rdd.isEmpty()) and WITHIN_BATCH_IF_EMPTY:
        hist_df = new_vec_df.select("episode_id", "embedding")
        used_within = True

    hist_pd = hist_df.toPandas()
    H_ids = hist_pd["episode_id"].astype(str).to_numpy() if len(hist_pd) else np.array([], str)
    H = (np.vstack(hist_pd["embedding"].to_numpy()).astype("float32")
         if len(hist_pd)
         else np.zeros((0, model_dim), "float32")
    )

    if H.shape[0]:
        H /= (np.linalg.norm(H, axis=1, keepdims=True) + 1e-8)
    else:
        log("[WARN] No historical vectors present. Skipping similarities.")

    sc = spark.sparkContext
    sc.broadcast(H_ids)
    sc.broadcast(H)
    K = TOP_K

    def part(rows: Iterable):
        import numpy as _np
        for r in rows:
            if H.shape[0] == 0:
                continue
            qid = r["episode_id"]
            x = _np.asarray(r["embedding"], dtype=_np.float32)
            n = _np.linalg.norm(x)
            if n == 0:
                continue
            x = x / n
            sims = H.dot(x)  # cosine similarity
            k = min(K, sims.shape[0])
            if k <= 0:
                continue
            idx = _np.argpartition(-sims, k-1)[:k]
            idx = idx[_np.argsort(-sims[idx])]
            for rank, j in enumerate(idx, start=1):     # <-- rank 1..k
                yield (qid, str(H_ids[j]), float(sims[j]), int(rank), int(K))

    schema = ("new_episode_id string, historical_episode_id string, similarity double, rank int, top_k int")
    pairs = spark.createDataFrame(
        new_vec_df.select("episode_id","embedding").rdd.mapPartitions(part), schema
    )
    return pairs, used_within


# ---------------- PIPELINE ----------------
def run_pipeline() -> None:
    # Create session inside the function (Airflow imports the module; avoid heavy globals)
    spark = get_spark("podcast-recs")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # 1) Load transcripts Delta table
    transcripts = spark.read.format("delta").load(DELTA_PATH_TRANSCRIPTS)
    required_cols = {"episode_id", "transcript"}
    missing = required_cols - set(transcripts.columns)
    if missing:
        raise RuntimeError(f"Missing required columns in transcripts table: {', '.join(missing)}")

    # 2) Decide what to (re)process
    # Approach: use the SINK (vectors) as truth -> left-anti join gives "pending"
    if RECOMPUTE_ALL:
        log("RECOMPUTE_ALL=True — embedding ALL transcripts for this model.")
        done = spark.createDataFrame([], "episode_id string")
    else:
        try:
            done = (spark.read.format("delta").load(DELTA_PATH_VECTORS)
                .where(col("model") == lit(MODEL_NAME))
                .select("episode_id").distinct())
        except AnalysisException:
            done = spark.createDataFrame([], "episode_id string")

# 3) Pending = transcripts \ done
    base = transcripts.select("episode_id", "transcript", *([ "date" ] if "date" in transcripts.columns else []))
    pending = base.join(done, on="episode_id", how="left_anti")

    if BATCH_DATE and "date" in pending.columns:
        pending = pending.filter(col("date") == lit(BATCH_DATE))

    if pending.rdd.isEmpty():
        spark.stop()
        return
    
    # 3) Embed transcripts into vector space (on driver)
    pdf = pending.toPandas()
    ep_ids = pdf["episode_id"].astype(str).tolist()
    texts = pdf["transcript"].fillna("").tolist()
    dates = pdf["date"].astype(str).tolist() if "date" in pdf.columns else [None] * len(ep_ids)

    # Load embedding model
    model = SentenceTransformer(MODEL_NAME, device=DEVICE)
    tok_ceiling = getattr(model.tokenizer, "model_max_length", MAX_TOKENS) or MAX_TOKENS
    model.max_seq_length = min(MAX_TOKENS, tok_ceiling)

    embedding_dim = model.get_sentence_embedding_dimension()

    vecs = []
    for t in texts:
        v = embed_long_document(t, model, MAX_TOKENS, OVERLAP, SAFETY_MARGIN, BATCH_SIZE)
        vecs.append(v if v is not None else np.zeros(embedding_dim, dtype="float32"))


    rows = [(ep_ids[i], dates[i], vecs[i].tolist(), MODEL_NAME) for i in range(len(ep_ids))]
    schema = "episode_id string, date string, embedding array<float>, model string"
    new_vec_df = (spark.createDataFrame(rows, schema)
                .withColumn("created_at", F.current_timestamp())
                .cache())

    # Ensure schema exists for vectors and similarities
    ensure_table(spark, DELTA_PATH_VECTORS, new_vec_df)

    empty_sims = spark.createDataFrame(
        [],
        "new_episode_id string, historical_episode_id string, "
        "similarity double, distance_embed double, k int, "
        "model string, created_at timestamp",
    )
    ensure_table(spark, DELTA_PATH_SIMILARITIES, empty_sims)

    # 4) Read history
    try:
        hist_all = spark.read.format("delta").load(DELTA_PATH_VECTORS).where(col("model") == MODEL_NAME)
        if BATCH_DATE and "date" in hist_all.columns:
            hist_df = hist_all.where(col("date") < lit(BATCH_DATE))
        else:
            hist_df = hist_all
        hist_df = hist_df.select("episode_id","embedding")
    except AnalysisException:
        hist_df = spark.createDataFrame([], "episode_id string, embedding array<float>")

    # 5) KNN similarity computation
    pairs_df, used_within = compute_topk_pairs(spark, new_vec_df, hist_df, embedding_dim)

    # 6) Format similarities output  (content-only)
    out_df = (
        pairs_df
        .filter(col("new_episode_id") != col("historical_episode_id"))
        .withColumn("model", lit(MODEL_NAME))
        .withColumn("distance_embed", 1.0 - F.col("similarity"))
        .withColumn("created_at", F.current_timestamp())  # TIMESTAMP
        .dropDuplicates(["new_episode_id", "historical_episode_id", "model"])
    )


# 6a) Write to MongoDB (append-only, time-series policy)
    wrote_sims = False
    try:
        (out_df.write
        .format("mongodb")
        .mode("append")
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", MONGO_COLLECTION_SIMILARITIES)
        .save())
        wrote_sims = True
    except Exception as e:
        log(f"[WARN] Mongo write failed ({e}) — falling back to Delta.")
        try:
            (out_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(DELTA_PATH_SIMILARITIES))
            wrote_sims = True
        except Exception as e2:
            log(f"[ERROR] Delta fallback also failed: {e2}")


# 7) Store new embeddings to Delta
    vec_target = DeltaTable.forPath(spark, DELTA_PATH_VECTORS)
    (vec_target.alias("t")
    .merge(new_vec_df.alias("s"),
            "t.episode_id = s.episode_id AND t.model = s.model")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

# 8) Mark episodes as processed
    num_vectors = len(rows)
    num_sims = out_df.count() if wrote_sims else 0

# 9) Stop session
    spark.stop()

if __name__ == "__main__":
    run_pipeline()

