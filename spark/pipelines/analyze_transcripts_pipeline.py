#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Analyze transcripts pipeline:
- Load transcripts (unprocessed by default; or all if RECOMPUTE_ALL=true)
- Token-safe chunking + embedding with SentenceTransformers
- KNN vs prior vectors; if no history, do within-batch KNN (self-matches removed)
- Write similarities -> MongoDB (fallback: Delta)
- Append vectors -> Delta
- Mark processed transcripts analyzed=true in Delta (merge on episode_id)
"""

import os
import datetime
import numpy as np

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.errors import AnalysisException
from pyspark.sql.types import DataType

from sentence_transformers import SentenceTransformer
from delta.tables import DeltaTable

from pyspark.sql import SparkSession
import os
def get_spark(app_name="podcast-recs"):
    delta_pkg = "io.delta:delta-spark_2.12:3.2.0"
    mongo_pkg = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    return (SparkSession.builder.appName(app_name)
            .config("spark.jars.packages", f"{delta_pkg},{mongo_pkg}")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.mongodb.write.connection.uri", mongo_uri)
            .config("spark.sql.shuffle.partitions","4")
            .getOrCreate())


# ---------------- Paths & Config ----------------
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

INPUT_DELTA_LAKE        = os.getenv("INPUT_DELTA_LAKE",        f"file://{BASE}/data/transcripts_demo")
VECTORS_DELTA_LAKE      = os.getenv("VECTORS_DELTA_LAKE",      f"file://{BASE}/data/delta/episode_vectors")
SIMILARITIES_DELTA_LAKE = os.getenv("SIMILARITIES_DELTA_LAKE", f"file://{BASE}/data/delta/similarities")

# Mongo sink
MONGO_URI        = os.getenv("MONGO_URI", "")   # e.g. mongodb://localhost:27017
MONGO_DB         = os.getenv("MONGO_DB", "podcasts")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "similarities")

# Batch/model settings
MODEL_NAME   = os.getenv("MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")  # 384-dim
TOP_K        = int(os.getenv("TOP_K", "3"))
MAX_TOKENS   = int(os.getenv("MAX_TOKENS", "512"))
OVERLAP      = int(os.getenv("OVERLAP", "32"))
SAFETY_MARGIN= int(os.getenv("TOKEN_SAFETY_MARGIN", "8"))  # extra headroom to avoid 259>256 re-tokenization
BATCH_DATE   = os.environ.get("BATCH_DATE")  # optional ISO date filter

# Behavior flags
RECOMPUTE_ALL          = os.getenv("RECOMPUTE_ALL", "false").lower() in ("1","true","yes")
WITHIN_BATCH_IF_EMPTY  = os.getenv("WITHIN_BATCH_IF_EMPTY", "true").lower() in ("1","true","yes")


# ---------------- Helpers ----------------
def df_is_empty(df):
    return df.rdd.isEmpty()

def col_exists(df, name: str) -> bool:
    return name in df.columns

def ensure_transcripts_columns(spark, table_path: str):
    """Add analyzed/analyzed_at columns to a Delta path-table if missing."""
    df = spark.read.format("delta").load(table_path)
    missing = [c for c in ["analyzed", "analyzed_at"] if c not in df.columns]
    if not missing:
        return
    # Build ALTER TABLE statement on a path-backed Delta table
    additions = []
    if "analyzed" in missing:
        additions.append("analyzed boolean")
    if "analyzed_at" in missing:
        additions.append("analyzed_at string")
    ddl = f"ALTER TABLE delta.`{table_path}` ADD COLUMNS ({', '.join(additions)})"
    spark.sql(ddl)

def chunk_text_by_tokens(text, tokenizer, max_tokens=256, overlap=32, safety_margin=8):
    """
    Split text into overlapping token windows by token ids, leaving room for special tokens
    and an extra safety margin to avoid decode->re-tokenize length drift.
    """
    if not text:
        return []

    t = tokenizer(
        text,
        add_special_tokens=False,
        return_attention_mask=False,
        return_token_type_ids=False,
        truncation=False,
    )
    ids = t.get("input_ids", [])
    if not ids:
        return []

    num_special = tokenizer.num_special_tokens_to_add(pair=False)
    tok_ceiling = getattr(tokenizer, "model_max_length", max_tokens) or max_tokens
    eff_max = min(max_tokens, tok_ceiling)

    # Room for special tokens + extra safety
    window = max(8, eff_max - num_special - safety_margin)
    stride = max(1, window - overlap)

    chunks = []
    for start in range(0, len(ids), stride):
        end = min(start + window, len(ids))
        if start >= end:
            break
        chunk_ids = ids[start:end]
        chunk_text = tokenizer.decode(chunk_ids, skip_special_tokens=True)
        if chunk_text.strip():
            chunks.append(chunk_text)
        if end >= len(ids):
            break
    return chunks

def embed_long_document(text, model, max_tokens=256, overlap=32, safety_margin=8):
    chunks = chunk_text_by_tokens(text, model.tokenizer, max_tokens, overlap, safety_margin)
    if not chunks:
        return None
    # Double-check (WITH special tokens) to be safe
    lens = [len(model.tokenizer(c, add_special_tokens=True)["input_ids"]) for c in chunks]
    if lens and max(lens) > model.max_seq_length:
        print(f"[WARN] A chunk would exceed max_seq_length: {max(lens)} > {model.max_seq_length}")
    embs = model.encode(chunks, normalize_embeddings=True)
    vec = np.mean(embs, axis=0)
    n = np.linalg.norm(vec) + 1e-8
    vec = vec / n
    return vec.astype("float32")

def upsert_analyzed_true(spark, table_path: str, ids):
    """
    Mark `analyzed=true` (and set analyzed_at) via Delta MERGE on episode_id,
    matching the episode_id type of the target table.
    """
    if not ids:
        return
    target = DeltaTable.forPath(spark, table_path)
    target_df = target.toDF().select("episode_id")
    target_dtype: DataType = target_df.schema["episode_id"].dataType

    updates = (
        spark.createDataFrame([(str(i),) for i in ids], "episode_id_str string")
        .select(
            col("episode_id_str").cast(target_dtype).alias("episode_id"),
            lit(True).alias("analyzed"),
            lit(datetime.datetime.utcnow().isoformat()).alias("analyzed_at"),
        )
    )

    (target.alias("t")
           .merge(updates.alias("u"), "t.episode_id = u.episode_id")
           .whenMatchedUpdate(set={"analyzed": "u.analyzed", "analyzed_at": "u.analyzed_at"})
           .execute())


# ---------------- Main ----------------
def run_pipeline():
    spark = get_spark()
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    sc = spark.sparkContext

    # 1) Read transcripts
    transcripts = spark.read.format("delta").load(INPUT_DELTA_LAKE)

    # Guards
    if not col_exists(transcripts, "episode_id"):
        spark.stop()
        raise RuntimeError("INPUT_DELTA_LAKE must contain column 'episode_id'.")
    if not col_exists(transcripts, "transcript"):
        spark.stop()
        raise RuntimeError("INPUT_DELTA_LAKE must contain column 'transcript'.")

    if not col_exists(transcripts, "date"):
        print("[WARN] No 'date' column in transcripts; all rows will be treated as same batch.")

    # 2) Select rows to process
    if col_exists(transcripts, "analyzed") and not RECOMPUTE_ALL:
        pending = transcripts.where((col("analyzed") == F.lit(False)) | col("analyzed").isNull())
    else:
        pending = transcripts

    if BATCH_DATE and col_exists(pending, "date"):
        pending = pending.where(col("date") == lit(BATCH_DATE))

    cols = ["episode_id", "transcript"] + (["date"] if col_exists(pending, "date") else [])
    pending = pending.select(*cols)

    if df_is_empty(pending):
        print("[INFO] No transcripts to analyze. (Either none exist, or all are already analyzed.)")
        spark.stop()
        return

    # 3) Collect to driver for embedding (demo scale)
    pdf = pending.toPandas()
    episode_ids = pdf["episode_id"].astype(str).tolist()
    texts       = pdf["transcript"].fillna("").tolist()
    dates       = pdf["date"].astype(str).tolist() if "date" in pdf.columns else [None] * len(episode_ids)

    # 4) Build model + set conservative max_seq_length
    model = SentenceTransformer(MODEL_NAME, device="cpu")
    tok_ceiling = getattr(model.tokenizer, "model_max_length", MAX_TOKENS) or MAX_TOKENS
    eff_max = min(MAX_TOKENS, tok_ceiling)
    model.max_seq_length = eff_max

    print(f"[INFO] Model={MODEL_NAME} | max_seq_length={model.max_seq_length} | "
          f"MAX_TOKENS={MAX_TOKENS} | OVERLAP={OVERLAP} | SAFETY_MARGIN={SAFETY_MARGIN}")

    # 5) Embed
    episode_vecs = []
    for t in texts:
        v = embed_long_document(t, model, max_tokens=MAX_TOKENS, overlap=OVERLAP, safety_margin=SAFETY_MARGIN)
        if v is None:
            v = np.zeros(model.get_sentence_embedding_dimension(), dtype="float32")
        episode_vecs.append(v)

    # 6) New vectors DF
    today_iso = datetime.date.today().isoformat()
    rows = [
        (episode_ids[i], dates[i], episode_vecs[i].tolist(), MODEL_NAME, today_iso)
        for i in range(len(episode_ids))
    ]
    new_vec_df = spark.createDataFrame(
        rows, schema="episode_id string, date string, embedding array<float>, model string, created_at string"
    ).cache()

    # 7) Historical vectors (same model; strictly older date if date exists)
    try:
        hist_df_all = spark.read.format("delta").load(VECTORS_DELTA_LAKE).where(col("model") == MODEL_NAME)
        if col_exists(hist_df_all, "date") and any(d is not None for d in dates):
            latest_date = max(d for d in dates if d is not None) if any(d is not None for d in dates) else None
            hist_df = hist_df_all.where(col("date") < lit(latest_date)) if latest_date else hist_df_all
        else:
            hist_df = hist_df_all
        hist_df = hist_df.select("episode_id", "embedding")
    except AnalysisException:
        hist_df = spark.createDataFrame([], "episode_id string, embedding array<float>")

    # 8) If no history, optionally do within-batch KNN (instead of exiting)
    use_within_batch = False
    if df_is_empty(hist_df):
        if WITHIN_BATCH_IF_EMPTY:
            print("[INFO] History empty — computing within-batch KNN (self-matches will be filtered) and then writing vectors.")
            hist_df = new_vec_df.select("episode_id", "embedding")
            use_within_batch = True
        else:
            (new_vec_df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(VECTORS_DELTA_LAKE))
            ensure_transcripts_columns(spark, INPUT_DELTA_LAKE)
            upsert_analyzed_true(spark, INPUT_DELTA_LAKE, episode_ids)
            print(f"[INFO] History empty. Stored {new_vec_df.count()} vectors and marked analyzed=true. Exiting.")
            spark.stop()
            return

    # 9) Broadcast historical matrix for cosine KNN
    hist_pd = hist_df.toPandas()
    H_ids = hist_pd["episode_id"].astype(str).to_numpy()
    H = np.vstack(hist_pd["embedding"].to_numpy()).astype("float32") if len(hist_pd) else np.zeros((0, model.get_sentence_embedding_dimension()), dtype="float32")
    if H.shape[0] == 0:
        print("[WARN] No historical vectors present after fallback. Skipping similarities.")
    else:
        H_norm = np.linalg.norm(H, axis=1, keepdims=True)
        H_norm[H_norm == 0] = 1.0
        H = H / H_norm

    sc.broadcast(H_ids)
    sc.broadcast(H)

    # 10) KNN(new, history) per partition
    new_arr_df = new_vec_df.select("episode_id", "embedding")
    TOP_K_LOCAL = TOP_K

    def topk_partition(rows):
        import numpy as _np
        for r in rows:
            if H.shape[0] == 0:
                continue
            qid = r["episode_id"]
            x = _np.array(r["embedding"], dtype=_np.float32)
            n = _np.linalg.norm(x)
            if n == 0:
                continue
            x = x / n
            sims = H.dot(x)
            k = min(TOP_K_LOCAL, sims.shape[0])
            if k <= 0:
                continue
            idx = _np.argpartition(-sims, k - 1)[:k]
            idx = idx[_np.argsort(-sims[idx])]
            for j in idx:
                yield (qid, str(H_ids[j]), float(1.0 - float(sims[j])), int(TOP_K_LOCAL))

    schema = "new_episode_id string, historical_episode_id string, distance double, k int"
    pairs_df = spark.createDataFrame(new_arr_df.rdd.mapPartitions(topk_partition), schema=schema)

    # Remove self-matches (relevant for within-batch mode)
    out_df = (
        pairs_df
        .where(col("new_episode_id") != col("historical_episode_id"))
        .withColumn("model", lit(MODEL_NAME))
        .withColumn("created_at", lit(today_iso))
        .dropDuplicates(["new_episode_id", "historical_episode_id"])
    )

    # 11) Write similarities (Mongo preferred; fallback to Delta)
    wrote_similarities = False
    try:
        writer = out_df.write.format("mongodb").mode("append")
        if MONGO_URI:
            writer = writer.option("uri", MONGO_URI)
        else:
            # if no full URI, rely on database/collection options; requires a default connector URI in Spark conf
            writer = writer.option("database", MONGO_DB).option("collection", MONGO_COLLECTION)
        writer.save()
        wrote_similarities = True
    except Exception as e:
        print(f"[WARN] Mongo write failed ({e}). Falling back to Delta similarities path.")
        try:
            (out_df
             .withColumnRenamed("created_at","written_at")
             .write.format("delta").mode("append").option("mergeSchema","true").save(SIMILARITIES_DELTA_LAKE))
            wrote_similarities = True
        except Exception as e2:
            print(f"[ERROR] Delta similarities fallback also failed: {e2}")

    # 12) Append new vectors so they become history next run
    (new_vec_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(VECTORS_DELTA_LAKE))

    # 13) Flip analyzed=true in transcripts
    ensure_transcripts_columns(spark, INPUT_DELTA_LAKE)
    upsert_analyzed_true(spark, INPUT_DELTA_LAKE, episode_ids)

    sims_ct = out_df.count() if wrote_similarities else 0
    print(f"[INFO] Stored {new_vec_df.count()} vectors; wrote {sims_ct} similarity rows"
          f"{' (within-batch)' if use_within_batch else ''}; marked {len(episode_ids)} episodes analyzed=true.")
    spark.stop()


# Entry point
if __name__ == "__main__":
    run_pipeline()
