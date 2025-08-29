# TRANSCRIPTS PART

- get podcasts 
- transcritps download
- training  - batch 

# Podcast Voice-to-Text Transcription Pipeline


[1] fetch_and_publish_metadata.py
    └── Fetches 50 podcast episodes
    └── Sends JSON metadata to Kafka: topic "metadata"

[2] kafka_transcription_consumer.py
    └── Reads from topic "metadata"
    └── Downloads and transcribes audio
    └── Saves result to "transcripts-en"  - delta lake + send back to kafka

.... Kafka Topics

- episode-metadata
  - Contains: full metadata for all new podcast episodes
  - Used by: MongoDB writer, Transcription service

- episode-ids
  - Contains: only episode IDs
  - Used by: downstream triggering (e.g., batch jobs)

    
## Overview

This pipeline fetches trending English-language podcasts and transcribes them into structured text using a local, fast, and private setup.

### What It Does

1. Fetch trending podcasts from the Podcast Index API  
2. Download one episode per show  
3. Convert audio to clean WAV format  
4. Transcribe audio with `faster-whisper` (OpenAI Whisper optimized)  
5. Output structured `.json` transcripts for each episode  

Everything runs locally without relying on external transcription services.

---

## Input

### `episodes.json`

Auto-generated file storing metadata for one episode per trending podcast.  
Each entry includes:

- `podcast_title`
- `episode_title`
- `audio_url`
- `description`
- `episode_id`

---

## Output

### `transcripts/` directory

Contains one `.json` file per episode with:

- Fully transcribed content
- Clean, chunk-stitched formatting
- Filenames based on sanitized episode titles (e.g., `Why_AI_Will_Change_Everything.json`)

---

## Key Features

- Single script handles both metadata fetching and transcription  
- English-language filtering (based on both metadata and audio detection)  
- Robust chunking to support long episodes  
- Parallel transcription using multiple processes  
- Local-only processing (no API keys or cloud infrastructure)

---

## Setup & Installation

### Prerequisites

- Python ≥ 3.8  
- `ffmpeg` (required by `pydub`)  
- `faster-whisper`

### Install Dependencies

```bash
# Install required Python packages
pip install -r requirements.txt

# If faster-whisper fails to install:
pip install git+https://github.com/guillaumekln/faster-whisper.git



Step                    Description
───────────────────────────────────────────────────────────────
1. Fetch Podcasts       → Retrieve trending shows from Podcast Index
2. Filter Language      → Keep only English podcasts
3. Fetch Episode        → Download one episode per podcast
4. Save Metadata        → Store in episodes.json
5. Download Audio       → Stream and prepare audio files
6. Convert to WAV       → Set to mono, 16kHz format for Whisper
7. Chunk Audio          → Split long episodes into 6-minute chunks
8. Transcribe Chunks    → Use faster-whisper for each chunk
9. Stitch Transcript    → Combine all chunks into one block of text
10. Save Transcript     → Write final .json to transcripts/



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




# User Events Notes

- **generation** (Kafka)
- **streaming / aggregation** (Spark → Delta)
- **training a recommendations** (ALS → Mongo)

## quick summary: 
We generate fake user events for podcast episodes from MongoDB and send them as JSON messages into a Kafka topic (one event per message).
Spark then streams these events from Kafka, assigns engagement weights, and aggregates daily engagement per user + episode into Delta Lake.
From Delta, we train an ALS recommendation model that learns user preferences and generates Top‑N recommended episodes per user.
Finally, we save these recommendations into MongoDB so we always have the latest personalized results.
Right now, retraining happens manually, but later we'll schedule it in Airflow? (idk about this part, right now: every time the ALS training script runs, it's overwriting the model...)

----------------------
## **1. Generate User Events**

**Script:** `spark/pipelines/generate_user_events_pipeline.py`

- Input: Episode IDs are fetched from MongoDB → used as valid new_episode_id values.
- Synthetic users: Creates NUM_USERS random user IDs (uuid4).
- Events generated: For each user, we randomly simulate actions on episodes: pause, like, skip, rate, complete.

*Message structure (Kafka)*	
- Each message sent to Kafka is a JSON object like: 
json<br>{<br> "event_id": "...",<br> "ts": "...",<br> "user_id": "...",<br> "new_episode_id": "...",<br> "event": "like",<br> "device": "ios",<br> "rating": 5,<br> "position_sec": 123,<br> "from_sec": 100,<br> "to_sec": 120,<br> "played_pct": 1.0<br>}

- Kafka topic: All events are streamed into one Kafka topic → TOPIC_USER_EVENTS_STREAMING. 
Each message is keyed by user_id so events for the same user stay in order.
- Storage: There’s no local storage — the raw events live only in Kafka until Spark consumes them.
---

## **2. Stream Events & Build Daily Engagement**

**Script:** `spark/pipelines/stream_user_events_pipeline.py`

- Input source:	Spark reads the Kafka topic TOPIC_USER_EVENTS_STREAMING. Each message = one event JSON from the generator.
- Parses fields: event_id, user_id, new_episode_id, event, rating, device, ts, position_sec, from_sec, to_sec, played_pct.

- Event scoring: Converts raw events → numeric engagement score
    - like → LIKE_W 
    - complete → COMPLETE_W 
    - rate → rating - 3 
    - pause → scaled fraction of listened time 
    - skip → SKIP_W

- Grouping logic: Spark groups events by user, episode, and day → calculates total engagement → saves it for training. 
    - sum(weight) → total engagement 
    - count(*) → number of events 
    - max(ts) → most recent activity

- Output storage: Writes aggregated daily engagement data into a Delta Lake table → DELTA_PATH_DAILY.

- Checkpointing: Uses USER_EVENT_STREAM directory to track processed Kafka offsets → ensures exactly-once processing.

## **3. Train Recommendations (ALS Model)**

**Script:** `spark/pipelines/train_user_recs_pipeline.py`

- Input data: Loads daily engagement data from Delta Lake (DELTA_PATH_DAILY).Each row = one user’s engagement score for one episode per day.
- Groups across days → sums engagement per (user_id, new_episode_id).
- Fits a new StringIndexer every time → captures new users and new episodes automatically.
- Trains a new ALS model from scratch.
- Generates Top-N recommendations.
- Overwrites MongoDB with fresh recommendations.
    - example of mongo inputs:

`recommendations> db.als_top_n.find()
| 
[
  {
    _id: ObjectId('68acc085007ec86e276b6b61'),
    user_id: '42423dac-043e-4c34-860e-11f3fbaf0276',
    new_episode_id: '41354833716',
    als_score: 1.0000100135803223
  },
  {
    _id: ObjectId('68acc085007ec86e276b6b62'),
    user_id: '42423dac-043e-4c34-860e-11f3fbaf0276',
    new_episode_id: '41350250744',
    als_score: 0.9996482729911804
  },
  {
    _id: ObjectId('68acc085007ec86e276b6b63'),
    user_id: '42423dac-043e-4c34-860e-11f3fbaf0276',
    new_episode_id: '910020010',
    als_score: 0.9990634322166443
  },
]
recommendations> `
    

!!! Because the script always reloads all data and re-fits everything, retraining is idempotent — we can run it as often as we want without breaking past data... idk if we schedule this in DAG - airflow too? 

# **What ALS Actually Does:**

- Input = Engagement Matrix
- Rows = users
- Columns = episodes
- Cells = summed engagement score (how much the user interacted with an episode)
- ALS algorithm (Alternating Least Squares):
    - Starts with random hidden “taste” vectors for users and episodes
    - Step 1: Fix user vectors → optimize episode vectors
    - Step 2: Fix episode vectors → optimize user vectors
    - Repeats until error is minimized
    - Produces a latent vector for each user and each episode

- Recommendations:
Predicted preference = dot product of user vector and episode vector.
Sort by score → take Top-N episodes per user.

## **4. Data Flow Overview**

```mermaid
flowchart LR
    A[MongoDB (Episodes)] -->|episode IDs| B[Event Generator]
    B -->|synthetic events| C[Kafka]
    C --> D[Spark Streaming]
    D -->|aggregated daily data| E[Delta Lake]
    E -->|training data| F[ALS Model Training]
    F -->|Top-N recs| G[MongoDB (User Recommendations)]
```

---

## **6. Storage Summary**

| **Stage**             | **Where data is stored**                 |
| --------------------- | ---------------------------------------- |
| Episode catalog       | MongoDB                                  |
| Raw events            | Kafka                                    |
| Daily engagement      | Delta Lake (`DELTA_PATH_DAILY`)          |
| Streaming checkpoints | Filesystem (`USER_EVENT_STREAM`)         |
| ALS model + mappings  | Filesystem (`ALS_MODEL_PATH`)            |
| Recommendations       | MongoDB (`MONGO_COLLECTION_USER_EVENTS`) |

