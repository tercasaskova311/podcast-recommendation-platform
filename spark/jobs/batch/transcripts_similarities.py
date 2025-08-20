# ---------------- CONFIG ----------------
#INPUT_DELTA_LAKE        = "/data_lake/transcripts_en"      # columns: episode_id, date, transcript, ...
#VECTORS_DELTA_LAKE      = "/data_lake/episode_vectors"     # columns: episode_id, model, embedding (array<float>), created_at
#SIMILARITIES_DELTA_LAKE = "/data_lake/similarities"        # columns: date, new_episode_id, historical_episode_id, distance, k, model
#TODAY                   = os.environ.get("BATCH_DATE", datetime.date.today().isoformat())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Embed the newest batch of transcripts, run exact KNN vs. historical vectors,
write KNN pairs to Mongo, and append vectors to Delta.

Fixes baked in:
- Token-safe chunking that reserves room for model special tokens (CLS/SEP).
- Set SentenceTransformers' max_seq_length coherently.
- Delta schema merge so we can keep `date` in the vectors table.
"""

import os
import datetime
import numpy as np

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit
from pyspark.errors import AnalysisException

from sentence_transformers import SentenceTransformer


# ---------------- Paths & Config ----------------
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))  # repo root

INPUT_DELTA_LAKE        = os.getenv("INPUT_DELTA_LAKE", f"file://{BASE}/data/delta/transcripts_en")
VECTORS_DELTA_LAKE      = os.getenv("VECTORS_DELTA_LAKE", f"file://{BASE}/data/delta/episode_vectors")
SIMILARITIES_DELTA_LAKE = os.getenv("SIMILARITIES_DELTA_LAKE", f"file://{BASE}/data/delta/similarities")

# Mongo sink (requires mongo-spark connector on spark-submit)
MONGO_DB          = os.getenv("MONGO_DB", "podcasts")
MONGO_COLLECTION  = os.getenv("MONGO_COLLECTION", "similarities")

# Batch/model settings
MODEL_NAME  = os.getenv("MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")  # 384-dim
TOP_K       = int(os.getenv("TOP_K", "3"))
MAX_TOKENS  = int(os.getenv("MAX_TOKENS", "256"))   # your intended belt width
OVERLAP     = int(os.getenv("OVERLAP", "32"))

# ---------------- Spark helpers ----------------
def get_spark():
    return (
        SparkSession.builder
        .appName("batch_knn_option_a")
        # Delta safety (works for local demos)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Let Delta add new columns if needed (we keep `date`)
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )


def df_is_empty(df):
    return df.rdd.isEmpty()


# ---------------- Embedding helpers ----------------
def chunk_text_by_tokens(text, tokenizer, max_tokens=256, overlap=32):
    """
    Split text into overlapping token windows, *reserving room for special tokens*
    that the encoder adds (e.g., CLS/SEP). Think: slice width < belt width.
    """
    if not text:
        return []

    t = tokenizer(
        text,
        add_special_tokens=False,   # encoder will add them
        return_attention_mask=False,
        return_token_type_ids=False,
        truncation=False,
    )
    ids = t.get("input_ids", [])
    if not ids:
        return []

    # How many tokens does the encoder add?
    num_special = tokenizer.num_special_tokens_to_add(pair=False)

    # The tokenizer's own ceiling (often 512); some models report large sentinels; clamp to max_tokens.
    eff_max = min(max_tokens, getattr(tokenizer, "model_max_length", max_tokens) or max_tokens)

    # Leave room for special tokens
    window = max(8, eff_max - num_special)

    # Sliding window stride
    stride = max(1, window - overlap)

    chunks = []
    for start in range(0, len(ids), stride):
        end = start + window
        chunk_ids = ids[start:end]
        if not chunk_ids:
            break
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

    # Safety: confirm re-tokenized lengths (after special tokens) fit the belt
    lens = [len(model.tokenizer(c, add_special_tokens=True)["input_ids"]) for c in chunks]
    if lens and max(lens) > model.max_seq_length:
        print(f"[WARN] A chunk would exceed max_seq_length: {max(lens)} > {model.max_seq_length}")

    embs = model.encode(chunks, normalize_embeddings=True)
    vec = np.mean(embs, axis=0)
    n = np.linalg.norm(vec) + 1e-8
    vec = vec / n
    return vec.astype("float32")


# ---------------- Main pipeline ----------------
def main():
    spark = get_spark()
    sc = spark.sparkContext

    # 1) Read transcripts (episode_id, date, transcript)
    transcripts = (
        spark.read.format("delta")
        .load(INPUT_DELTA_LAKE)
        .select("episode_id", "transcript", "date")
    )

    # Select batch date
    env_batch_date = os.environ.get("BATCH_DATE")
    if env_batch_date:
        has_date = not df_is_empty(transcripts.where(col("date") == env_batch_date))
        if not has_date:
            available = transcripts.agg(F.min("date").alias("min"), F.max("date").alias("max")).collect()[0]
            spark.stop()
            raise RuntimeError(
                f"BATCH_DATE={env_batch_date} not found in transcripts. "
                f"Available date range: {available['min']} .. {available['max']}"
            )
        latest_date = env_batch_date
    else:
        latest_date = transcripts.agg(F.max("date")).collect()[0][0]
        if latest_date is None:
            spark.stop()
            raise RuntimeError("No transcripts found in INPUT_DELTA_LAKE.")

    print(f"[INFO] Running batch for date={latest_date}")

    # New transcripts to embed
    new_transcripts = transcripts.filter(col("date") == latest_date).cache()

    # 2) Embed new transcripts on driver (small batches recommended)
    pdf = new_transcripts.toPandas()
    if pdf.empty:
        spark.stop()
        raise RuntimeError(f"No transcripts for date={latest_date}.")

    episode_ids = pdf["episode_id"].astype(str).tolist()
    texts = pdf["transcript"].fillna("").tolist()

    # Build model and make its belt width explicit
    model = SentenceTransformer(MODEL_NAME, device="cpu")
    num_special = model.tokenizer.num_special_tokens_to_add(pair=False)
    tok_ceiling = getattr(model.tokenizer, "model_max_length", MAX_TOKENS) or MAX_TOKENS
    eff_max = min(MAX_TOKENS, tok_ceiling)
    model.max_seq_length = eff_max  # encoder belt width

    print(f"[INFO] Token settings: MAX_TOKENS={MAX_TOKENS}, tokenizer_ceiling={tok_ceiling}, "
          f"num_special={num_special}, model.max_seq_length={model.max_seq_length}")

    episode_vecs = []
    for t in texts:
        v = embed_long_document(t, model, max_tokens=MAX_TOKENS, overlap=OVERLAP)
        if v is None:
            v = np.zeros(model.get_sentence_embedding_dimension(), dtype="float32")
        episode_vecs.append(v)

    # Prepare new vectors DF (keep `date` + `created_at`)
    today_iso = datetime.date.today().isoformat()
    new_rows = [
        (episode_ids[i], latest_date, episode_vecs[i].tolist(), MODEL_NAME, today_iso)
        for i in range(len(episode_ids))
    ]
    new_vec_df = spark.createDataFrame(
        new_rows,
        schema="episode_id string, date string, embedding array<float>, model string, created_at string",
    ).cache()

    # 3) Load historical vectors for same model, strictly older dates
    try:
        hist_df = (
            spark.read.format("delta")
            .load(VECTORS_DELTA_LAKE)
            .where((col("model") == MODEL_NAME) & (col("date") < lit(latest_date)))
            .select("episode_id", "embedding")
        )
    except AnalysisException:
        hist_df = spark.createDataFrame([], "episode_id string, embedding array<float>")

    # If history empty, bootstrap: just store vectors and exit
    if df_is_empty(hist_df):
        (new_vec_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")   # allow `date` on first write
            .save(VECTORS_DELTA_LAKE))
        print(f"[INFO] History empty. Stored {new_vec_df.count()} new vectors for {latest_date}. Exiting.")
        spark.stop()
        return

    # 4) Broadcast historical matrix
    hist_pd = hist_df.toPandas()
    H_ids = hist_pd["episode_id"].astype(str).to_numpy()
    H = np.vstack(hist_pd["embedding"].to_numpy()).astype("float32")

    # L2-normalize history
    H_norm = np.linalg.norm(H, axis=1, keepdims=True)
    H_norm[H_norm == 0] = 1.0
    H = H / H_norm

    bc_H_ids = sc.broadcast(H_ids)
    bc_H = sc.broadcast(H)

    # 5) Exact KNN(new, history) per partition
    new_arr_df = new_vec_df.select("episode_id", "embedding")
    TOP_K_LOCAL = TOP_K  # capture for closure

    def topk_partition(rows):
        import numpy as _np
        H = bc_H.value
        H_ids = bc_H_ids.value
        kmax = TOP_K_LOCAL

        for r in rows:
            qid = r["episode_id"]
            x = _np.array(r["embedding"], dtype=_np.float32)
            n = _np.linalg.norm(x)
            if n == 0 or H.shape[0] == 0:
                continue
            x = x / n
            sims = H.dot(x)  # cosine similarity
            k = min(kmax, sims.shape[0])
            if k <= 0:
                continue
            idx = _np.argpartition(-sims, k - 1)[:k]
            idx = idx[_np.argsort(-sims[idx])]
            for j in idx:
                # cosine distance = 1 - cosine similarity
                yield (qid, str(H_ids[j]), float(1.0 - float(sims[j])), int(kmax))

    schema = "new_episode_id string, historical_episode_id string, distance double, k int"
    pairs_df = spark.createDataFrame(new_arr_df.rdd.mapPartitions(topk_partition), schema=schema)

    # 6) Write to Mongo (and optionally to Delta if you want)
    out_df = (
        pairs_df
        .withColumn("date", lit(latest_date))
        .withColumn("model", lit(MODEL_NAME))
        .dropDuplicates(["new_episode_id", "historical_episode_id"])
    )

    # Mongo sink
    (
        out_df.write
        .format("mongodb")
        .mode("append")
        .option("database",  MONGO_DB)
        .option("collection", MONGO_COLLECTION)
        .save()
    )

    # (Optional) Also store similarities to Delta:
    # (
    #   out_df.write.format("delta").mode("append").option("mergeSchema","true").save(SIMILARITIES_DELTA_LAKE)
    # )

    # 7) Append new vectors so they become history next run (keep `date`)
    (new_vec_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(VECTORS_DELTA_LAKE))

    print(f"[INFO] Wrote {out_df.count()} similarity rows for date={latest_date} (topK={TOP_K}).")
    spark.stop()


if __name__ == "__main__":
    main()
