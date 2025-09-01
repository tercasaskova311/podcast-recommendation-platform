# Architecture 

**Ingestion**

* **Transcripts:** PodcastIndex → **Kafka** → ASR (**Fast-Whisper**) → **Delta**.
* **User events:** Simulated/real events → Kafka/**Spark Streaming** → **Delta**.

**Model training**

* **Behavior:** **ALS (Spark ML)** on event aggregates → **MongoDB** (`als_top_n`).
* **Content:** **Embeddings + KNN** on transcripts → **MongoDB** (`similarities`).

**Recommendation**

* **Final ranking:** Weighted join (ALS + similarity) → **user–episode** pairs → **MongoDB** (`final_recommendations`).

**Serving**

* **App:** **Streamlit** reads MongoDB for recs.
* **Analytics:** **DuckDB** reads **Delta** (events/transcripts) for fast, lightweight charts.

---

## Why These Technologies?

### Kafka

* **Role:** Event transport (ingestion + replay).
* **Why:** Back-pressure friendly, consumer groups for parallel workers, *replay by offset* for deterministic reprocessing.
* **Trade-offs:** Not a data lake; keep retention sane and land truth in Delta.

### Delta Lake 

* **Role:** ACID data lake for transcripts, events, and embeddings.
* **Why:** Schema evolution, time-travel for reproducibility (train on version N), scalable Parquet under the hood.
* **Trade-offs:** Best with columnar/append patterns; use merge/upsert judiciously.

### Spark (+ Structured Streaming)

* **Role:** Batch ETL, streaming aggregation, and **ML (ALS)**.
* **Why:** One engine for stream + batch; integrates with Delta; distributed ALS scales beyond single-machine RAM.
* **Trade-offs:** Spin-up/overhead for tiny reads—use DuckDB for interactive analytics.

### Vosk — “efficient transcription”

* **Role:** Speech-to-text for podcast audio.
* **Why:** Optimized inference (CTranslate2), strong accuracy for long-form audio, **local** (no vendor lock-in), fits batch pipeline.
* **Alt considered:** Cloud STT (faster to start, \$\$\$ at scale), WhisperX (alignment/timestamps) if you need word-level timing.

### Embeddings + KNN 

* **Role:** Turn text into vectors → find semantically similar episodes.
* **Why:** Solves **cold-start** and topical discovery; complements behavior-only models.
* **Trade-offs:** Choose model size vs. latency; maintain versioned embeddings.

### ALS (Spark ML) 

* **Role:** Collaborative filtering from user–episode interactions.
* **Why:** Well-understood, scalable on Spark, handles implicit feedback (weights from plays/likes).
* **Alt considered:** BPR/MF variants, neural recommenders; ALS wins on simplicity + scaling for this phase.

### MongoDB 

* **Role:** Low-latency store for app-facing results (similarities, ALS, final recs).
* **Why:** JSON-first, simple overwrite/update patterns, easy indexing for user/episode queries.
* **Trade-offs:** Keep heavy analytics out; that’s what Delta/DuckDB/Spark are for.

### DuckDB 

* **Role:** In-process SQL over Delta/Parquet for dashboards and ad-hoc analysis.
* **Why:** Zero cluster, columnar speed, ideal for Streamlit; avoids waking Spark.
* **Trade-offs:** Single-process memory; great for reads, not a transactional store.

### Streamlit

* **Role:** UI for recommendations + live analytics.
* **Why:** Rapid dev, Python-native, easy to wire Mongo + DuckDB.
* **Trade-offs:** For heavier frontends/APIs you may outgrow it.

---

## Data 

* **Kafka topics:**
  `episode-metadata` (podcast/episode JSON), `user-events` (like/play/skip…).
* **Delta tables:**
  `episodes`, `transcripts`, `user_vs_episode_daily`, `vectors` (embeddings).
* **Mongo collections:**
  `similarities`, `als_top_n`, `final_recommendations`.

---

## Modeling 

**Event weighting (implicit feedback → ALS):**

* Example: `play_>80% = 3`, `like = 2`, `play_<20% = 0.5`, `skip = 0.2`.
* Aggregate to daily `user×episode` scores before training ALS.

**Hybrid recommendation (ALS ⊕ Content):**

* `final_score = α·als_score + (1–α)·cosine_similarity`, with `α≈0.6–0.8` to start.
* Calibrate by validation clicks or simulated CTR.

---

## Performance & Scaling

* **What breaks first:** the **ASR pipeline** (audio → text) when episode volume spikes—compute & I/O heavy.
* **Mitigations:** batched downloads, GPU/CPU pools, chunking, retry queues, and Airflow backfills.
* **Read efficiency:** use **DuckDB** for small, interactive panels; reserve **Spark** for heavy transforms/training.

---

## Example

1. New episode lands → Kafka → **Vosk** → transcript → **Delta**.
2. Embeddings computed → **KNN** similar episodes → **Mongo** (`similarities`).
3. User events stream in → Spark aggregates → **ALS** top-N → **Mongo** (`als_top_n`).
4. **Final join** (ALS + similarity) → **Mongo** (`final_recommendations`) → **Streamlit** renders.

---

## Key Files

* **Ingestion:** `scripts/batch/new_episodes_transcript_download.py` (calls `util/transcription.process_batch`).
* **Transcripts analysis:** `spark/pipelines/analyze_transcripts_pipeline.py`.
* **Events streaming:** `spark/pipelines/streaming_user_events_pipeline.py`.
* **ALS training:** `spark/pipelines/training_user_events_pipeline.py`.
* **Fusion:** `spark/pipelines/final_recommendation.py`.
* **Dashboard:** `dashboard.py` (Mongo + DuckDB).

---

