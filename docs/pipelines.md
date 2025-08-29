# Podcast Transcription & Recommendation — Pipelines (Structured Notes)

A modular set of pipelines to fetch podcast metadata, transcribe audio, analyze transcripts, simulate user interactions, and generate personalized podcast recommendations using **collaborative filtering (ALS)** + **embedding-based transcript similarity**.

---

## Pipeline Overview

### Pipeline 1: **Transcripts**

1. Fetch episodes + metadata → **PodcastIndex API → Kafka**
2. Batch job (Airflow): **Voice-to-Text** (Fast Whisper)
3. NLP: **TF‑IDF + Embeddings** → compute KNN similarities
4. Save similarities → **MongoDB** (later joined in final stage)

### Pipeline 2: **User Events**

1. Generate **synthetic user events** (likes, plays, skips)
2. Stream to **Delta Lake** (via Kafka → Spark Streaming)
3. Train **ALS recommender** (Spark ML) → top‑N episodes/user
4. Save outputs → **MongoDB**

### Pipeline 3: **Final Recommendation**

* `final_recommendation.py` loads **similarities** + **ALS recs** (from MongoDB)
* Joins them via **weighted average** → **final rankings**
* Stored in **MongoDB** → served in **Streamlit dashboard**

---

##  Transcription Phase

### Steps

* Fetch metadata → Kafka (`episode-metadata` topic)
* Consume metadata → download + transcribe audio → Delta Lake (`transcripts-en`)
* Publish transcript results back to Kafka

**Topics:**

* `episode-metadata` → full episode details for transcription + downstream storage
* `episode-ids` → IDs for deduplication/triggers

**Logic:**

* Convert audio (16kHz mono WAV)
* Transcribe using `faster-whisper`
* Save transcripts JSON → Delta
* Retry failed transcripts ≤3 times

---

##  Transcript Analysis — `analyze_transcripts_pipeline.py`

1. Load transcripts from Delta
2. Embed via **SentenceTransformers**
3. Compute top‑K similar episodes
4. Save similarities → MongoDB (primary), Delta (backup)

**Schemas:**

* Transcripts → `episode_id, transcript, analyzed, timestamp`
* Embeddings → `episode_id, embedding[], model, date`
* Similarities → `{episode_A, episode_B, cosine_similarity}`

---

##  User Events & ALS Recommendations

1. **Generate events** (`generate_user_events_pipeline.py`) → produce playback/like/skip messages into Kafka
2. **Stream & aggregate** (`stream_user_events_pipeline.py`) → Spark aggregates engagement per user/episode → Delta
3. **Train ALS model** (`train_user_recs_pipeline.py`) → learn latent vectors → Top‑N episodes/user → MongoDB

**Event schema:**

```json
{
  "event_id": "...",
  "user_id": "...",
  "episode_id": "...",
  "event": "like|skip|play",
  "played_pct": 1.0
}
```

---

##  Storage Summary

| Stage           | Storage                             |
| --------------- | ----------------------------------- |
| Metadata        | Kafka, Delta, MongoDB               |
| Transcripts     | Delta (`transcripts-en`)            |
| Events          | Kafka (raw), Delta (aggregated)     |
| Embeddings      | Delta (`vectors`)                   |
| Similarities    | MongoDB (primary), Delta (fallback) |
| Recommendations | MongoDB (`als_top_n`, `final_recs`) |

---

##  Dashboard (Streamlit)

* Reads: **MongoDB recommendations** + **Delta (via DuckDB)**
* Features: live user engagement, top episodes, filters, auto‑refresh
* Run:

```bash
make dashboard-up
```

---

##  Troubleshooting Tips

* **Kafka**: ensure topics exist & keyed properly
* **Docker/Streamlit**: fix `volumes` or port conflicts
* **Delta errors**: check path + permissions
* **Mongo**: validate URI, credentials
