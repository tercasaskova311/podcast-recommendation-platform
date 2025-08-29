# Podcast Transcription & Recommendation: Pipeline

A modular pipelines to fetch podcast metadata, transcribe audio, analyze transcripts, simulate user interactions, and generate personalized podcast recommendations using collaborative filtering (ALS) and embeddign transcripts to get transcrip similarities.

---

## Pipeline Overview
# Pipeline 1: Transcripts
[1] podcastindex.com API
    └──> fetch episodes + metadata
           └──> send to Kafka

[2] Batch job: Voice-to-Text (ASR)
    └──> get transcript for each episode

[3] NLP: TF-IDF + Embeddings
    └──> compute content similarity (KNN)

[4] Store similarities in MongoDB
    └──> used later in final_recommendation.py

# Pipeline 2: Userr_events
[1] Generate artificial user events
    └──> simulate playback/like/etc. based on transcript metadata

[2] Stream events to Delta Lake
    └──> live interaction logs

[3] Train ALS recommender in Spark
    └──> output: top N episodes per user

[4] Store user-item recommendations in MongoDB
    └──> used in final_recommendation.py

# Pipelien 3: Final recommendation
final_recommendation.py:
    ├── Load content similarity (from MongoDB)
    ├── Load ALS recommendations (from MongoDB)
    └── JOIN both sources:
         Weighted average of similarity + ALS score
           ⇒ Final recommended episodes per user

[Save to MongoDB] => [Load into Streamlit Dashboard]


### Describtion of pipelines

### 1. Transcription Phase

#### Steps

* Fetch podcasts
* Download transcripts
* Run batch training

#### Workflow

```
fetch_and_publish_metadata.py
  └── Fetches 50 podcast episodes
  └── Sends JSON metadata to Kafka ("episode-metadata")

kafka_transcription_consumer.py
  └── Reads metadata from Kafka
  └── Downloads and transcribes audio
  └── Stores transcripts to Delta Lake ("transcripts-en")
  └── Publishes back to Kafka
```

### Kafka Topics

* `episode-metadata`: Full episode metadata used by transcription + Mongo writer
* `episode-ids`: Episode IDs for deduplication / triggers

---

## Transcription Logic

1. **Fetch metadata** from Podcast Index API
2. **Download one episode** per show (audio only)
3. **Convert** audio to WAV (mono, 16kHz)
4. **Transcribe** using `faster-whisper`
5. **Store** transcripts as `.json` locally or in Delta format

### Input: `episodes.json`

Each entry contains:

* `podcast_title`
* `episode_title`
* `audio_url`
* `description`
* `episode_id`

### Output: `transcripts/`

Each `.json` contains:

* Full transcript
* Clean formatting
* Sanitized filename

### Features

* Parallel transcription
* Token-aware chunking
* Language filtering
* Local-only (no APIs needed)

### Setup

```bash
pip install -r requirements.txt
# If needed
pip install git+https://github.com/guillaumekln/faster-whisper.git
```

---

## Transcripts Analysis: `analyze_transcripts_pipeline.py`

Converts raw transcripts to embeddings and similarity pairs.

### Steps

1. Load transcripts from Delta
2. Embed using SentenceTransformers
3. Compute top-K similar episodes
4. Write:

   * Similarities to MongoDB or Delta fallback
   * Embeddings to Delta
5. Mark episodes as analyzed

### Config

From `config.settings`:

* Embedding model: `MODEL_NAME`
* Storage: Delta paths, Mongo URI
* Batch control: `TOP_K`, `BATCH_DATE`, etc.

### Data Schema

**Transcripts:**

```txt
episode_id: string
transcript: string
analyzed: bool
analyzed_at: string
```

**Embeddings:**

```txt
episode_id: string
embedding: array<float>
model: string
date: string
created_at: string
```

**Similarities:**

```json
{
  "new_episode_id": "A",
  "historical_episode_id": "B",
  "cosine_similarity": 0.5991,
  "model": "all-MiniLM-L6-v2",
  "created_at": "2025-08-22"
}
```

---

## User Event Generation & ALS Recommendations

### 1. Generate Events: `generate_user_events_pipeline.py`

* Creates fake events for episodes from MongoDB
* Sends events (like, skip, rate, etc.) to Kafka topic

**Kafka Message:**

```json
{
  "event_id": "...",
  "user_id": "...",
  "new_episode_id": "...",
  "event": "like",
  "played_pct": 1.0
}
```

### 2. Stream & Aggregate: `stream_user_events_pipeline.py`

* Reads from Kafka topic
* Assigns weights to each event type
* Aggregates daily engagement per user/episode to Delta

### 3. Train ALS Model: `train_user_recs_pipeline.py`

* Loads daily engagement
* Trains ALS (Alternating Least Squares) model
* Predicts top-N episodes per user
* Stores output in MongoDB

### ALS Logic:

* Matrix of users vs. episodes with engagement scores
* Learns latent vectors for users & episodes
* Recommends by dot product → Top-N

---

##  Storage Summary

| Stage           | Storage                             |
| --------------- | ----------------------------------- |
| Metadata        | Kafka, Delta, MongoDB               |
| Transcripts     | Delta (`transcripts-en`)            |
| Events          | Kafka (stream), Delta (aggregated)  |
| Embeddings      | Delta (`vectors`)                   |
| Similarities    | MongoDB (primary), Delta (fallback) |
| Recommendations | MongoDB (`als_top_n`)               |

---

##  Dashboard (Streamlit)

### Reads:

* MongoDB recommendations
* Delta Lake transcripts + events via DuckDB

### Run:

```bash
make dashboard-up
make dashboard-logs
```

---

## Troubleshooting Tips

* Kafka: ensure topic exists and keying is correct
* Docker/Streamlit path errors → fix `volumes` and `context`
* Port conflicts → kill local `streamlit` or change port
* Delta errors → confirm file paths and permissions
* Mongo connection → check URI, user permissions




