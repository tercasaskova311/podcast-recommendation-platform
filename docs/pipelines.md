# Podcast Transcription & Recommendation — Pipelines 

A modular set of pipelines to fetch podcast metadata, transcribe audio, analyze transcripts, simulate user interactions, and generate personalized podcast recommendations using **collaborative filtering (ALS)** + **embedding-based transcript similarity**.

---

## Pipeline Overview

### Pipeline 1: **Transcripts**

1. Fetch episodes + metadata → **PodcastIndex API → Kafka**
2. Batch job (Airflow): **Voice-to-Text** (Vosk)
3. NLP: **TF‑IDF + Embeddings** → compute KNN similarities (SentencTransformer)
4. Save similarities → **MongoDB** (later joined in final stage)

### Pipeline 2: **User Events**

1. Generate **synthetic user events** (likes, plays, skips) → episode_id from **MongoBD**(similarities)
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
- Fetch from PodcastIndex.com (open API - daily charts of most listened podcasts across different platforms)
* Consume metadata → download + transcribe audio → Delta Lake (`transcripts-en`)
- We fetch english only podcasts 
* Publish transcript results back to Kafka

**Topics:**

* `episode-metadata` → full episode details for transcription + downstream storage
* `episode-id` → IDs for deduplication/triggers

**Logic:**

* Convert audio (16kHz mono WAV)
* Transcribe using `Vosk`
- Vosk STT Service uses vosk-api (opens new window) to perform offline speech-to-text in openHAB. Vosk (opens new window) is an offline open source speech recognition toolkit. It enables speech recognition for 20+ languages and dialects.
* Save transcripts JSON → Delta
* Retry failed transcripts ≤3 times

---

##  Transcript Analysis — `analyze_transcripts_pipeline.py`

1. Load transcripts from Delta
2. Embed via **SentenceTransformers**
3. Compute top‑K similar episodes via KNN
4. Save similarities → MongoDB (primary), Delta (backup)

**Schemas:**

* Transcripts → `episode_id, transcript, analyzed, timestamp`
* Embeddings → `episode_id, historical_episode_id, model, date`
* Similarities → `{episode_A, episode_B, embed_distance(cosine_similarity)}`

---

##  User Events & ALS Recommendations

1. **Generate events** (`generate_user_events_pipeline.py`) → produce playback/like/skip messages into Kafka
- Events are produced based on MongoDB - similarities. We get episode_id from Mongo collection in order to simulate real life schema.
2. **Stream & aggregate** (`stream_user_events_pipeline.py`) → Spark aggregates engagement per user/episode → Delta
3. **Train ALS model** (`train_user_recs_pipeline.py`) → learn latent vectors → Top‑N episodes/user → MongoDB

## Alternating Least Squares (ALS) for training user events

### What is ALS
Spark ML’s **ALS** factorizes a sparse user–episode matrix into low-dimensional **user-preferences** vectors and **episode-theme** vectors; relevance is the dot product \(x_u^\top y_i\).
---

### Why ALS for our data: 
- **Sparsity:** treats missing pairs as unknowns; does **not** enumerate zeros → scales to big catalogs.
- **Implicit-feedback:** events → **preference** \(p_{ui}\in\{0,1\}\) and **confidence** \(c_{ui}=1+\alpha r_{ui}\); stronger engagement ⇒ higher weight.
- **Ridge-like stability:** each alternating step is a regularized least squares; robust to noise/collinearity.

---

### Objective 
\[
\min_{X,Y}\;\sum_{u,i} c_{ui}\,\big(p_{ui}-x_u^\top y_i\big)^2
\;+\;\lambda\!\left(\sum_u\|x_u\|_2^2+\sum_i\|y_i\|_2^2\right),
\quad
p_{ui}=\mathbf{1}[r_{ui}>0],\; c_{ui}=1+\alpha r_{ui}.
\]

---

### Data: 
- **Source columns:** `user_id`, `episode_id`, `day`, `engagement`, `num_events`, `last_ts`, …
- **Aggregate:** sum `engagement` per `(user_id, episode_id)`.
- **Denoise:** keep pairs with `engagement > MIN_ENGAGEMENT` (we use **1e-6**).
- **Indexing:** map `user_id → user_idx`, `episode_id → item_idx` (dense integers).
- **Cleansing:** cast to numeric; drop nulls/negatives; ensure indices ≥ 0.

---

### Current defaults: 
- **rank:** **32** (latent dimension)  
- **regParam (λ):** **0.08** (L2 penalty)  
- **maxIter:** **10** (alternations)  
- **implicitPrefs:** **true** (use \(p,c\) weighting)  
- **alpha:** **40.0** (confidence scale)  
- **nonnegative:** **true** (factors ≥ 0; helpful for counts)  
- **coldStartStrategy:** **"drop"** (filter NaNs during eval)  
- **blocks/checkpoint:** `numUserBlocks=4`, `numItemBlocks=4`, `checkpointInterval=2`  
- **minimum signals:** `MIN_ENGAGEMENT=1e-6`, `MIN_HISTORY_ENGAGEMENT=1e-6`

---

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

* Reads: **MongoDB final recommendations** + **User Events Analytics from Delta (via DuckDB)**
* Features: live user engagement, top episodes, filters, auto‑refresh
* Run:

```bash
make dashboard-up
```

**Streamlit** UI  
**DuckDB** vectorized SQL into local/remote Delta. Raw user events (`ts`, `user_id`, `episode_id`, `event`, `rating`, …).

**Mongo** is the store of final recommendations.

---

## Why DuckDB?
- **Delta Lake, no Spark cluster needed.** DuckDB’s `delta` extension lets us read Delta tables directly (even on S3 with `httpfs`) for **low-latency ad-hoc analytics**.
- **Speed on small/medium interactive slices.** In-process, vectorized, with smart predicate pushdown (e.g., “last X minutes”) → sub-second queries for the dashboard window.
- **SQL-first ergonomics.** One place to do **typing, filtering, joining, and time-windowing** before sending tidy frames to the UI.

## Why Streamlit?
- **UI** without a full frontend stack: sliders, tables, charts in a few lines.
- **Built-in caching.** `cache_resource` for long-lived clients (DuckDB connection, Mongo client), `cache_data` for short-lived frames (TTL seconds).  

**Metrics & Views**
   - **KPI cards**
     - Users with recommendations: `nunique(user_id)` from recs.
     - Avg recommendation score: mean of `score`.
     - Most liked episode in window: mode of `event == "like"` grouped by `episode_title`.
   - **Users → Top-1 recommendation**  
     - Per user: keep the **highest-score** episode.  
     - **Inner join** with user profiles (drop recs for users without a profile).  
     - Show **Top-20** by score, then sort by last name, first name for readability.
   - **Live engagement (line chart)**  
     - Floor `ts` to minutes; plot **events per minute** across the window.
   - **Top episodes by engagement (weighted)**
     - Weight map (example): `{like: 2.0, complete: 3.0, pause: 0.5, rate: 1.0, skip: −1.0}`.
     - Aggregate `engagement_score = Σ weight(event)` per `episode_title`; break ties by event count.
   - **Top rated (Bayesian smoothing)**
     - For `rate` events only:  
       - Per episode: average rating \(R\) and votes \(v\).  
       - Global mean \(C\).  
       - **Weighted rank** \( \textbf{wr} = \frac{v}{v+m}R + \frac{m}{v+m}C \) (with prior \(m\), e.g., 5).  
       - Sort by `wr` (then `votes`) to avoid “1 vote at 5★” topping the chart.
---

##  Troubleshooting Tips

* **Kafka**: ensure topics exist & keyed properly
* **Docker/Streamlit**: fix `volumes` or port conflicts
* **Delta errors**: check path + permissions
* **Mongo**: validate URI, credentials
