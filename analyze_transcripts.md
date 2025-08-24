### analyze_transcripts_pipeline.py NOTES ###

- This pipeline turns raw transcripts into embeddings, then for every new embeddings, it finds the K closest neighbors on the sphere and stores the relationships for fast recommendation queries.

1) What this pipeline does 
- Load transcripts from a Delta table.
- Embed each transcript using a SentenceTransformer (chunk → embed → weighted average on the sphere).
- Find nearest neighbors: for each new embedding, compute top-K most similar historical episodes (cosine distance).
- Write results
- Similarities → MongoDB (primary) with automatic Delta fallback.
- Embeddings → Delta (append).
- Mark episodes as analyzed in the transcripts table.
- Log a summary and stop the Spark session.

2) Folder  & how to run
The script lives under a package like: spark/pipelines/analyze_transcripts_pipeline.py.
It imports config and utilities from sibling modules (..util.common, ..util.delta, ..config.settings).

Run
# from project root
python -m spark.pipelines.analyze_transcripts_pipeline
Spark packages you’ll need
Delta Lake (to read/write Delta tables).
MongoDB Spark Connector (to write similarities to MongoDB).
Sentence Transformers (model + tokenizer).

*via spark-submit, you include packages, e.g.:
--packages io.delta:delta-spark_2.12:<version>,org.mongodb.spark:mongo-spark-connector_2.12:<version>

3) Configuration (what each setting means)
Imported from ..config.settings:
Model & embedding
MODEL_NAME — e.g., "all-MiniLM-L6-v2".
MAX_TOKENS — chunk token ceiling.
OVERLAP — how much successive chunks overlap (to avoid cutting thoughts mid-sentence).
SAFETY_MARGIN — reserved room so the tokenizer’s special tokens never overflow.
BATCH_SIZE — embedding batch size on the driver.
DEVICE — "cpu" or "cuda" for SentenceTransformer.
Storage paths
DELTA_PATH_TRANSCRIPTS — source Delta of transcripts (must contain episode_id, transcript, optionally date).
DELTA_PATH_VECTORS — destination Delta for embeddings.
DELTA_PATH_SIMILARITIES — destination Delta fallback for similarities.
MongoDB
MONGO_URI, MONGO_DB, MONGO_COLLECTION — where similarities are written first.
Batching & recompute
RECOMPUTE_ALL — ignore analyzed and recompute everything when True.
WITHIN_BATCH_IF_EMPTY — if there’s no history yet, compute neighbors within the current batch (self-matches are filtered).
BATCH_DATE — if provided and a date column exists, restrict to that date (e.g., run “yesterday’s batch”).
TOP_K — how many nearest neighbors to keep.

4) Data model (schemas)
Transcripts (input Delta)
Required: episode_id, transcript
Optional: date
Automatically added if missing: analyzed (bool), analyzed_at (string)
Vectors (output Delta)
episode_id string
date string (pass-through if present)
embedding array<float> (unit-normalized vector)
model string
created_at string (ISO date, e.g., "2025-08-22")
Similarities (MongoDB primary, Delta fallback)
new_episode_id string
historical_episode_id string
distance double (= 1 − cosine_similarity)
k int (the K you asked for; helpful for QA and downstream logic)
model string
created_at string

Why store model? Different models put points in different places on the sphere. You don’t want to mix neighbors computed from different geometries.

5) Logic, step by step (with intuition)
5.1 Token-aware chunking (chunk_text_by_tokens)
The tokenizer returns token IDs for the whole transcript.
We slide a window of size window = min(MAX_TOKENS, tok_ceiling) − (special tokens + SAFETY_MARGIN) with stride window − OVERLAP.
Chunks are sized so the model never truncates, and overlaps let thoughts that fall on a boundary be included in both chunks.

5.2 Embed a long document (embed_long_document)
Embed each chunk → unit vectors on the sphere.
Compute token-count weights per chunk. Longer chunks count more.
Take a weighted mean of chunk vectors on the sphere and re-normalize.

5.3 Historical vs. within-batch KNN (compute_topk_pairs)
Load historical (episode_id, embedding) where model == MODEL_NAME.
If BATCH_DATE exists, keep only history strictly earlier than that date (prevents “peeking”).
Broadcast the historical matrix H and its H_ids to executors.
For each new embedding x (already unit length), compute sims = H · x.
Take the top-K indices with argpartition (efficient partial sort).
Yield pairs (new_episode_id, historical_episode_id, distance = 1 − sim, k = TOP_K).
Within-batch fallback: If there’s no history yet and WITHIN_BATCH_IF_EMPTY=True, we use the current batch as history (and later filter self-matches). This bootstraps recommendations on Day 1.

6) MongoDB for similarities (and what your mongosh output shows)

- writes similarities to MongoDB first:
// Example query API  run
db.similarities.find(
  { new_episode_id: "41301767506", model: "all-MiniLM-L6-v2" }
).sort({ distance: 1 }).limit(5)
Your mongosh session confirms exactly those documents:
{
  new_episode_id: '41301767506',
  historical_episode_id: '41302041808',
  distance: 0.17609679698944092,
  k: 5,
  model: 'all-MiniLM-L6-v2',
  created_at: '2025-08-22'
}


7) End-to-end flow (pseudocode storyboard)
Load transcripts t ← Delta at DELTA_PATH_TRANSCRIPTS.
Select pending rows (not analyzed) and optionally filter by BATCH_DATE.
Embed on driver: Pandas collect → chunk → embed → weighted sphere average.
Write vectors to Delta (append; ensure table exists).
Read historical vectors (same model, strictly earlier than BATCH_DATE if set).
Top-K pairs via broadcast dot-product; drop self-matches.
Write similarities to Mongo (primary), Delta (fallback).
Mark analyzed and log summary.

8) Running locally — a concrete example
Suppose today is 2025-08-22 and you have 100 new transcripts.
Set:
MODEL_NAME="all-MiniLM-L6-v2"
MAX_TOKENS=256
OVERLAP=32
SAFETY_MARGIN=8
TOP_K=5
BATCH_DATE="2025-08-22"
Ensure your transcripts Delta has: episode_id, transcript, and (optionally) date="2025-08-22" for these rows.
Start local MongoDB (mongod) with no auth (dev).
Run the module.
Sanity checks:
Delta /vectors got 100 rows with embedding length equal to the model dimension.
Mongo db.similarities.find({model: "all-MiniLM-L6-v2"}).limit(2) shows pairs like in your output.
Transcripts table now has analyzed=true for those episode_ids.
