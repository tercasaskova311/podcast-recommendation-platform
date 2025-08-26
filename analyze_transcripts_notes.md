# `analyze_transcripts_pipeline.py` — NOTES

This pipeline turns **raw transcripts** into **embeddings**, finds their **nearest neighbors**, and stores **similarity pairs** for recommendation.

**Workflow**:

1. **Load** transcripts from Delta Lake.
2. **Embed** each transcript with a SentenceTransformer:

   * split into token-aware chunks,
   * embed each chunk,
   * compute a weighted average on the unit sphere.
3. **Find neighbors**: for each new embedding, find the top-K most similar historical episodes (cosine distance).
4. **Write outputs**:

   * Similarities → MongoDB (primary) with Delta fallback,
   * Embeddings → Delta,
   * Mark episodes as `analyzed`.
5. **Log summary** and stop Spark.

---

## How to Run

From project root:

```bash
python -m spark.pipelines.analyze_transcripts_pipeline
```

You need Spark with:

* Delta Lake
* MongoDB Spark Connector
* SentenceTransformers

Example `spark-submit`:

```bash
--packages io.delta:delta-spark_2.12:<version>, \
           org.mongodb.spark:mongo-spark-connector_2.12:<version>
```

---

## Configuration (from `config.settings`)

**Embedding**

* `MODEL_NAME` — e.g., `"all-MiniLM-L6-v2"`.
* `MAX_TOKENS`, `OVERLAP`, `SAFETY_MARGIN` — chunking strategy.
* `BATCH_SIZE`, `DEVICE` — embedding batch size and device (cpu/gpu).

**Storage paths**

* `DELTA_PATH_TRANSCRIPTS` — source transcripts Delta.
* `DELTA_PATH_VECTORS` — embeddings Delta.
* `DELTA_PATH_SIMILARITIES` — Delta fallback for similarities.

**MongoDB**

* `MONGO_URI`, `MONGO_DB`, `MONGO_COLLECTION`.

**Batching**

* `RECOMPUTE_ALL` — re-analyze all when true.
* `WITHIN_BATCH_IF_EMPTY` — if no history, use current batch.
* `BATCH_DATE` — restrict to one date (if transcript table has `date`).
* `TOP_K` — number of nearest neighbors.

---

### Transcripts (input Delta)

```txt
episode_id: string
transcript: string

analyzed: bool  
analyzed_at: string
```

### Embeddings (Delta)

```txt
episode_id: string
date: string
embedding: array<float>   # unit-normalized
model: string
created_at: string
```

### Similarities (MongoDB / Delta fallback)

```json
{
  "new_episode_id": "A",        
  "historical_episode_id": "B",     
  "model": "all-MiniLM-L6-v2",
  "distance_embed": 0.4009,        // 1 - cosine_similarity
  "cosine_similarity": 0.5991,
  "created_at": ISODate("2025-08-22")
}
```
---

## Logic (Step by Step)

1. **Chunk transcripts**

   * Sliding token window with overlap.
   * Prevents truncation, keeps semantic continuity.

2. **Embed**

   * Each chunk → unit vector.
   * Weighted average by token length.
   * Re-normalize to stay on the sphere.

3. **KNN neighbors**

   * Load historical embeddings.
   * Broadcast to executors.
   * Dot product = cosine similarity.
   * Take top-K, drop self-matches.
   * If no history: fallback to within-batch neighbors.

4. **Write outputs**

   * Similarities → MongoDB first, Delta fallback if Mongo fails.
   * Embeddings → Delta.
   * Mark transcripts as analyzed.

---

## MongoDB Usage

### Find top neighbors for one episode

```js
db.similarities.find(
  { anchor_episode_id: "41301767506", model: "all-MiniLM-L6-v2" }
).sort({ distance_embed: 1 }).limit(5)
```

### Aggregate top-5 neighbors for all episodes

```js
db.similarities.aggregate([
  { $match: { model: "all-MiniLM-L6-v2" } },
  { $sort: { anchor_episode_id: 1, distance_embed: 1 } },
  { $group: {
      _id: "$anchor_episode_id",
      top5: {
        $push: {
          ep: "$candidate_episode_id",
          sim: "$cosine_similarity"
        }
      }
  }},
  { $project: { _id: 0, anchor_episode_id: "$_id", 
                top5: { $slice: ["$top5", 5] } } }
])
```

### Useful Indexes

```js
db.similarities.createIndex(
  { anchor_episode_id: 1, model: 1, cosine_similarity: -1 }
)
db.similarities.createIndex({ candidate_episode_id: 1, model: 1 })
db.similarities.createIndex({ created_at: 1 })
```

---

EXAMPLE:
```js
test> use podcasts
switched to db podcasts
podcasts> db.similarities.aggregate([
|   { $match: { model: "all-MiniLM-L6-v2" } },
|   { $addFields: { similarity_embed: { $subtract: [1, "$distance_embed"] } } },
|   { $sort: { new_episode_id: 1, distance_embed: 1 } },
|   { $group: {
|       _id: "$new_episode_id",
|       top5: {
|         $push: {
|           hist: "$historical_episode_id",
|           dist: "$distance_embed",
|           sim: "$similarity_embed"
|         }
|       }
|   }},
|   { $project: { _id: 0, new_episode_id: "$_id", top5: { $slice: ["$top5", 5] } } },
|   { $limit: 10 }
| ]).pretty()
| 
[
  {
    new_episode_id: '41354196646',
    top5: [
      {
        hist: '41350250744',
        dist: 0.40091651678085327,
        sim: 0.5990834832191467
      },
      {
        hist: '41355735923',
        dist: 0.42300236225128174,
        sim: 0.5769976377487183
      },
      {
        hist: '41355546495',
        dist: 0.47727012634277344,
        sim: 0.5227298736572266
      },
      {
        hist: '41354833716',
        dist: 0.5228027403354645,
        sim: 0.4771972596645355
      }
    ]
  },
  ```


  Delta: transcripts                           Embedding step                         Delta: vectors
───────────────────────────┐                ┌──────────────────────────────┐        ┌─────────────────────────────────────────────┐
episode_id (id) ───────────┼──────────────► │ carried over                 │ ─────► │ episode_id (id)                            │
transcript (text) ─────────┼─ tokenize ──► │ embed_long_document(text)    │ ─────► │ embedding : array<float>  (NEW)            │
date (opt) ────────────────┼──────────────► │ carried over (as string)     │ ─────► │ date : string (optional)                   │
analyzed (bool?) ──────────┤                │ —                            │        │ model : string (e.g. all-MiniLM-L6-v2)     │
analyzed_at (string?) ─────┘                │ —                            │        │ created_at : timestamp (NEW)               │
                                           └──────────────────────────────┘        └─────────────────────────────────────────────┘
                                                                                                    │
                                                                                                    │  (read history vectors)
                                                                                                    ▼
                                                                              KNN over vectors (new vs. history)
                                                                              ┌────────────────────────────────────────────┐
                                                                              │ new_episode_id : string      (from new)   │
                                                                              │ historical_episode_id : str  (from hist)  │
                                                                              │ similarity OR distance_embed (NEW)        │
                                                                              │ rank : int (1..k)            (RECOMMEND)  │
                                                                              │ k : int (TOP_K)                           │
                                                                              │ model : string (from vectors)             │
                                                                              │ created_at : timestamp        (NEW)       │
                                                                              └────────────────────────────────────────────┘
                                                                                           │
                                    (primary path)                                         │ (fallback if Mongo down)
                                    ▼                                                      ▼
                        MongoDB: similarities (append)                             Delta: similarities (append/merge)
