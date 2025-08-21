"""
Analyze transcripts pipeline (uses util/ helpers; same behavior):
- Load transcripts (unprocessed by default; or all if RECOMPUTE_ALL=true)
- Token-safe chunking + embedding (SentenceTransformers)
- KNN vs prior vectors; within-batch KNN when history empty
- Write similarities -> MongoDB (Spark connector; fallback: Delta)
- Append vectors -> Delta
- Mark processed transcripts analyzed=true in Delta (merge on episode_id)
"""

from __future__ import annotations
import os, datetime
from typing import Iterable, List, Optional, Sequence, Tuple
import numpy as np

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.errors import AnalysisException
from pyspark.sql.types import DataType
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable
from sentence_transformers import SentenceTransformer
from ..util.common import get_spark
from ..util.delta import _ensure_table as ensure_table        
from ..util.mongo import write_to_mongo                     

# ---------------- Configuration ----------------
def _env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).lower() in ("1","true","yes","y")

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

INPUT_DELTA_LAKE        = os.getenv("INPUT_DELTA_LAKE",        f"file://{BASE}/data/transcripts_demo")
VECTORS_DELTA_LAKE      = os.getenv("VECTORS_DELTA_LAKE",      f"file://{BASE}/data/delta/episode_vectors")
SIMILARITIES_DELTA_LAKE = os.getenv("SIMILARITIES_DELTA_LAKE", f"file://{BASE}/data/delta/similarities")

MONGO_URI        = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB         = os.getenv("MONGO_DB", "podcasts")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "similarities")

MODEL_NAME    = os.getenv("MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
TOP_K         = int(os.getenv("TOP_K", "3"))
MAX_TOKENS    = int(os.getenv("MAX_TOKENS", "512"))
OVERLAP       = int(os.getenv("OVERLAP", "32"))
SAFETY_MARGIN = int(os.getenv("TOKEN_SAFETY_MARGIN", "8"))
BATCH_DATE    = os.getenv("BATCH_DATE")

RECOMPUTE_ALL         = _env_bool("RECOMPUTE_ALL", False)
WITHIN_BATCH_IF_EMPTY = _env_bool("WITHIN_BATCH_IF_EMPTY", True)

def log(msg: str) -> None: print(f"[INFO] {msg}")

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

def embed_long_document(text: str, model: SentenceTransformer, max_tokens=256, overlap=32, safety_margin=8):
    chunks = chunk_text_by_tokens(text, model.tokenizer, max_tokens, overlap, safety_margin)
    if not chunks: return None
    lens = [len(model.tokenizer(c, add_special_tokens=True)["input_ids"]) for c in chunks]
    if lens and max(lens) > model.max_seq_length:
        print(f"[WARN] chunk exceeds model limit: {max(lens)} > {model.max_seq_length}")
    embs = model.encode(chunks, normalize_embeddings=True)
    vec = embs.mean(axis=0)
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

# ---------------- Orchestration ----------------
def run_pipeline() -> None:
    # Config echo (prevents “env drift” bugs)
    print("[CONFIG] INPUT_DELTA_LAKE       =", INPUT_DELTA_LAKE)
    print("[CONFIG] VECTORS_DELTA_LAKE     =", VECTORS_DELTA_LAKE)
    print("[CONFIG] SIMILARITIES_DELTA_LAKE=", SIMILARITIES_DELTA_LAKE)
    print("[CONFIG] Mongo =", MONGO_URI, MONGO_DB, MONGO_COLLECTION)

    spark = get_spark("podcast-recs")      
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # 1) Read transcripts
    transcripts = spark.read.format("delta").load(INPUT_DELTA_LAKE)
    if "episode_id" not in transcripts.columns:
        raise RuntimeError("INPUT_DELTA_LAKE must contain 'episode_id'.")
    if "transcript" not in transcripts.columns:
        raise RuntimeError("INPUT_DELTA_LAKE must contain 'transcript'.")
    if "date" not in transcripts.columns:
        print("[WARN] No 'date' column in transcripts; all rows treated as one batch.")

    # 2) Select rows to process
    pending = (transcripts.where((col("analyzed") == F.lit(False)) | col("analyzed").isNull())
               if ("analyzed" in transcripts.columns and not _env_bool("RECOMPUTE_ALL", False))
               else transcripts)
    if BATCH_DATE and "date" in pending.columns:
        pending = pending.where(col("date") == lit(BATCH_DATE))
    cols = ["episode_id","transcript"] + (["date"] if "date" in pending.columns else [])
    pending = pending.select(*cols)
    if pending.rdd.isEmpty():
        log("No transcripts to analyze. (Either none exist, or all are already analyzed.)")
        spark.stop(); return

    # 3) Embed on driver (demo scale)
    pdf = pending.toPandas()
    ep_ids = pdf["episode_id"].astype(str).tolist()
    texts  = pdf["transcript"].fillna("").tolist()
    dates  = pdf["date"].astype(str).tolist() if "date" in pdf.columns else [None]*len(ep_ids)

    model = SentenceTransformer(MODEL_NAME, device="cpu")
    tok_ceiling = getattr(model.tokenizer, "model_max_length", MAX_TOKENS) or MAX_TOKENS
    model.max_seq_length = min(MAX_TOKENS, tok_ceiling)
    log(f"Model={MODEL_NAME} | max_seq_length={model.max_seq_length} | MAX_TOKENS={MAX_TOKENS} | OVERLAP={OVERLAP} | SAFETY_MARGIN={SAFETY_MARGIN}")

    vecs = []
    for t in texts:
        v = embed_long_document(t, model, MAX_TOKENS, OVERLAP, SAFETY_MARGIN)
        vecs.append(v if v is not None else np.zeros(model.get_sentence_embedding_dimension(), "float32"))

    today = datetime.date.today().isoformat()
    rows = [(ep_ids[i], dates[i], vecs[i].tolist(), MODEL_NAME, today) for i in range(len(ep_ids))]
    new_vec_df = spark.createDataFrame(rows, "episode_id string, date string, embedding array<float>, model string, created_at string").cache()

    # Ensure vector/similarity tables exist with correct schema
    ensure_table(spark, VECTORS_DELTA_LAKE, new_vec_df)
    ensure_table(spark, SIMILARITIES_DELTA_LAKE,
                 spark.createDataFrame([], "new_episode_id string, historical_episode_id string, distance double, k int, model string, created_at string"))

    # 4) Read history
    try:
        hist_all = spark.read.format("delta").load(VECTORS_DELTA_LAKE).where(col("model") == MODEL_NAME)
        if "date" in hist_all.columns and any(d is not None for d in dates):
            latest = max(d for d in dates if d is not None) if any(d is not None for d in dates) else None
            hist_df = hist_all.where(col("date") < lit(latest)) if latest else hist_all
        else:
            hist_df = hist_all
        hist_df = hist_df.select("episode_id","embedding")
    except AnalysisException:
        hist_df = spark.createDataFrame([], "episode_id string, embedding array<float>")

    # 5) KNN
    pairs_df, used_within = compute_topk_pairs(spark, new_vec_df, hist_df, model.get_sentence_embedding_dimension())

    # 6) Write similarities (Mongo primary; Delta fallback)
    out_df = (pairs_df
              .where(col("new_episode_id") != col("historical_episode_id"))
              .withColumn("model", lit(MODEL_NAME))
              .withColumn("created_at", lit(today))
              .dropDuplicates(["new_episode_id","historical_episode_id"]))

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
        log("Wrote similarities to MongoDB.")
    except Exception as e:
        print(f"[WARN] Mongo write failed ({e}). Falling back to Delta similarities path.")
        try:
            (out_df.write.format("delta").mode("append").option("mergeSchema","true").save(SIMILARITIES_DELTA_LAKE))
            wrote_sims = True
            log("Wrote similarities to Delta fallback.")
        except Exception as e2:
            print(f"[ERROR] Delta similarities fallback also failed: {e2}")

    # 7) Append vectors
    (new_vec_df.write.format("delta").mode("append").option("mergeSchema","true").save(VECTORS_DELTA_LAKE))

    # 8) Flip analyzed=true
    ensure_transcripts_columns(spark, INPUT_DELTA_LAKE)
    upsert_analyzed_true(spark, INPUT_DELTA_LAKE, ep_ids)

    sims_ct = out_df.count() if wrote_sims else 0
    log(f"Stored {new_vec_df.count()} vectors; wrote {sims_ct} similarity rows"
        f"{' (within-batch)' if used_within else ''}; marked {len(ep_ids)} episodes analyzed=true.")
    spark.stop()

if __name__ == "__main__":
    run_pipeline()
