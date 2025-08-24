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

from ..util.common import get_spark
from ..util.delta import _ensure_table as ensure_table        
from ..util.mongo import write_to_mongo  
# python -m spark.pipelines.analyze_transcripts_pipeline
# --- Config ---
from ..config.settings import (
    MODEL_NAME, MAX_TOKENS, OVERLAP, SAFETY_MARGIN, BATCH_SIZE, DEVICE,
    DELTA_PATH_TRANSCRIPTS, DELTA_PATH_VECTORS, DELTA_PATH_SIMILARITIES,
    MONGO_URI, MONGO_DB, MONGO_COLLECTION,
    RECOMPUTE_ALL, WITHIN_BATCH_IF_EMPTY, BATCH_DATE, TOP_K
)

def log(msg: str, level: str = "INFO") -> None:
    print(f"[{level}] {msg}")

def ensure_transcripts_columns(spark: SparkSession, table_path: str) -> None:
    """Add analyzed/analyzed_at if missing."""
    df = spark.read.format("delta").load(table_path)
    missing = [c for c in ("analyzed", "analyzed_at") if c not in df.columns]
    if not missing: return
    adds = []
    if "analyzed" in missing: adds.append("analyzed boolean")
    if "analyzed_at" in missing: adds.append("analyzed_at string")
    spark.sql(f"ALTER TABLE delta.`{table_path}` ADD COLUMNS ({', '.join(adds)})")

def upsert_analyzed_true(spark: SparkSession, table_path: str, ids: Sequence[str]) -> None:
    if not ids: return
    tgt = DeltaTable.forPath(spark, table_path)
    dtype: DataType = tgt.toDF().schema["episode_id"].dataType
    updates = (spark.createDataFrame([(str(i),) for i in ids], "episode_id_str string")
                     .select(col("episode_id_str").cast(dtype).alias("episode_id"),
                             lit(True).alias("analyzed"),
                             lit(datetime.datetime.utcnow().isoformat()).alias("analyzed_at")))
    (tgt.alias("t")
        .merge(updates.alias("u"), "t.episode_id = u.episode_id")
        .whenMatchedUpdate(set={"analyzed": "u.analyzed", "analyzed_at": "u.analyzed_at"})
        .execute())

# ---------------- Embedding helpers ----------------
def chunk_text_by_tokens(text: str, tokenizer, max_tokens=256, overlap=32, safety_margin=8) -> List[str]:
    if not text: return []
    t = tokenizer(text, add_special_tokens=False, return_attention_mask=False,
                  return_token_type_ids=False, truncation=False)
    ids = t.get("input_ids", [])
    if not ids: return []
    num_special = tokenizer.num_special_tokens_to_add(pair=False)
    tok_ceiling = getattr(tokenizer, "model_max_length", max_tokens) or max_tokens
    eff_max = min(max_tokens, tok_ceiling)
    window = max(8, eff_max - num_special - safety_margin)
    stride = max(1, window - overlap)
    out = []
    for start in range(0, len(ids), stride):
        end = min(start + window, len(ids))
        if start >= end: break
        txt = tokenizer.decode(ids[start:end], skip_special_tokens=True).strip()
        if txt: out.append(txt)
        if end >= len(ids): break
    return out

def embed_long_document(text: str, model: SentenceTransformer,
                        max_tokens=256, overlap=32, safety_margin=8, batch_size=64):
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

    # Final renorm to keep cosine distance meaningful
    vec /= (np.linalg.norm(vec) + 1e-8)
    return vec.astype("float32")


# ---------------- KNN ----------------
def compute_topk_pairs(spark: SparkSession, new_vec_df: DataFrame, hist_df: DataFrame,
                       model_dim: int) -> Tuple[DataFrame, bool]:
    used_within = False
    if (hist_df.rdd.isEmpty()) and WITHIN_BATCH_IF_EMPTY:
        log("History empty — using within-batch KNN (self-matches will be filtered).")
        hist_df = new_vec_df.select("episode_id", "embedding"); used_within = True

    hist_pd = hist_df.toPandas()
    H_ids = hist_pd["episode_id"].astype(str).to_numpy() if len(hist_pd) else np.array([], str)
    H = (np.vstack(hist_pd["embedding"].to_numpy()).astype("float32")
         if len(hist_pd) else np.zeros((0, model_dim), "float32"))
    if H.shape[0]:
        H /= (np.linalg.norm(H, axis=1, keepdims=True) + 1e-8)
    else:
        print("[WARN] No historical vectors present. Skipping similarities.")

    sc = spark.sparkContext
    sc.broadcast(H_ids); sc.broadcast(H)
    K = TOP_K

    def part(rows: Iterable):
        import numpy as _np
        for r in rows:
            if H.shape[0] == 0: continue
            qid = r["episode_id"]
            x = _np.asarray(r["embedding"], dtype=_np.float32)
            n = _np.linalg.norm(x)
            if n == 0: continue
            x = x / n
            sims = H.dot(x)
            k = min(K, sims.shape[0])
            if k <= 0: continue
            idx = _np.argpartition(-sims, k-1)[:k]
            idx = idx[_np.argsort(-sims[idx])]
            for j in idx:
                yield (qid, str(H_ids[j]), float(1.0 - float(sims[j])), int(K))

    schema = "new_episode_id string, historical_episode_id string, distance double, k int"
    pairs = spark.createDataFrame(new_vec_df.select("episode_id","embedding").rdd.mapPartitions(part), schema)
    return pairs, used_within

# ---------------- PIPELINE ----------------
def run_pipeline() -> None:
    spark = get_spark("podcast-recs")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # 1) Load transcripts Delta table
    transcripts = spark.read.format("delta").load(DELTA_PATH_TRANSCRIPTS)
    required_cols = {"episode_id", "transcript"}
    missing = required_cols - set(transcripts.columns)
    
    if missing:
        raise RuntimeError(f"Missing required columns in transcripts table: {', '.join(missing)}")
    
    if "date" not in transcripts.columns:
        log("No 'date' column found in transcripts — entire dataset will be treated as a single batch", level="WARN")

    # 2) Select rows to process
    if "analyzed" in transcripts.columns and not RECOMPUTE_ALL:
        pending = transcripts.filter((col("analyzed") == False) | col("analyzed").isNull())
    else:
        pending = transcripts

    if BATCH_DATE and "date" in pending.columns:
        pending = pending.filter(col("date") == lit(BATCH_DATE))

    selected_cols = ["episode_id", "transcript"]
    if "date" in pending.columns:
        selected_cols.append("date")

    pending = pending.select(*selected_cols)

    if pending.rdd.isEmpty():
        log("No transcripts to analyze. (Either none exist, or all are already analyzed.)", level="INFO")
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

    

    # Embed each transcript
    embedding_dim = model.get_sentence_embedding_dimension()
    vecs = []
    for t in texts:
        v = embed_long_document(t, model, MAX_TOKENS, OVERLAP, SAFETY_MARGIN, batch_size=BATCH_SIZE)
        vecs.append(v if v is not None else np.zeros(embedding_dim, dtype="float32"))

    # Create output DataFrame
    today = datetime.date.today().isoformat()
    rows = [
        (ep_ids[i], dates[i], vecs[i].tolist(), MODEL_NAME, today)
        for i in range(len(ep_ids))
    ]
    schema = "episode_id string, date string, embedding array<float>, model string, created_at string"
    new_vec_df = spark.createDataFrame(rows, schema).cache()

    # Ensure schema exists for vectors and similarities
    ensure_table(spark, DELTA_PATH_VECTORS, new_vec_df)
    ensure_table(
        spark,
        DELTA_PATH_SIMILARITIES,
        spark.createDataFrame(
            [], "new_episode_id string, historical_episode_id string, distance double, k int, model string, created_at string"
        ),
    )

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
    embedding_dim = model.get_sentence_embedding_dimension()
    pairs_df, used_within = compute_topk_pairs(spark, new_vec_df, hist_df, embedding_dim)

    # 6) Format similarities output  (content-only)
    out_df = (
        pairs_df
        .filter(col("new_episode_id") != col("historical_episode_id"))
        .withColumn("model", lit(MODEL_NAME))
    # store embed-sim clearly named; keep distance for back-compat if you like
        .withColumnRenamed("distance", "distance_embed")
        .withColumn("created_at", F.to_utc_timestamp(F.current_timestamp(), "UTC"))
    # avoid cross-model collapse; include model in the dedupe key
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
        .option("collection", MONGO_COLLECTION)
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
    new_vec_df.write.format("delta").mode("append").option("mergeSchema", "true").save(DELTA_PATH_VECTORS)

# 8) Mark episodes as processed
    ensure_transcripts_columns(spark, DELTA_PATH_TRANSCRIPTS)
    upsert_analyzed_true(spark, DELTA_PATH_TRANSCRIPTS, ep_ids)

# Summary log
    num_vectors = new_vec_df.count()
    num_sims = out_df.count() if wrote_sims else 0
    log(
        f"Stored {num_vectors} vectors; "
        f"Wrote {num_sims} similarity rows"
        f"{' (within-batch fallback)' if used_within else ''}; "
        f"Marked {len(ep_ids)} episodes as analyzed."
    )

# 9) Stop session
    spark.stop()

if __name__ == "__main__":
    run_pipeline()

