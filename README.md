# Podcast Analytics & Recommendation Platform

This project implements a scalable data platform for analyzing podcast content, tracking user behavior, and providing intelligent recommendations to listeners and content producers.

End-to-end podcast ingestion, transcription, similarity/recs, and a **Streamlit** dashboard. The stack uses **Kafka** (events), **Delta Lake** (storage), **DuckDB** (fast windowed reads), **Spark** (batch), **Airflow** (orchestration), and **MongoDB** (serving layer).

---

## Project Overview

The system ingests both **real-time events** (from users) and **batch transcripts + metadata** (about podcasts), processes the data using **Apache Spark**, stores it in **Delta Lake**, and then exposes structured insights and personalized recommendations via **MongoDB** to be consumed by frontend applications.

---

## Architecture

### Ingest

* Fetch trending episodes from PodcastIndex.com → publish to Kafka (`episode-metadata`, `episode-ids`)
* Sequential downloader/transcriber writes **episodes** & **transcripts** to **Delta**
* User Event simulation - in order to recreate real life data (Stream processing)

### Training / Analytics

* Aggregate **user events** (stream/micro-batch) into **Delta**
* Train **user events** via ALS (Spark ML) to get episode recommendation based on user events

* Batch procesing: Loading transcript text data from Delta and training them 
* Traning transcript data - output: text embedings = text similarities, recomputed each time batch is running 


### Serving

* **MongoDB** stores the `final_recommendations` data which combine similarities recommendation + user event recommendation
* **Streamlit** reads **Mongo** (final_recommendation) + **Delta** (user_events “last N minutes”, episodes metadata) via **DuckDB**

---

## Architecture Diagram

![Architecture Diagram](./docs/project_architecture.png)

---

## Technologies Used

* **Runtime:** Python, Spark
* **Storage:** Delta Lake, MongoDB
* **Messaging:** Kafka
* **Orchestration:** Airflow
* **Serving:** MongoDB, Streamlit
* **Fast ad-hoc SQL over files:** DuckDB

---

## Prerequisites

* **Docker Desktop** (with Compose)
* Free local ports for services (Kafka, Mongo, Spark UI, Airflow UI, Streamlit)
* If your network uses a corporate/VPN proxy, ensure Docker can pull from Docker Hub (see **Troubleshooting**)

---

## Instructions to Run the Infrastructure

### Initialize the system

1. **Start Docker**, then from the repo root:

   ```bash
   make init
   ```

   This:

   * builds/starts **Kafka**, **Mongo**, **Airflow** (and Spark build)
   * waits for **Kafka** quorum and **creates topics** (correct partitions/retention)
   * **initializes MongoDB indexes**
   * starts the **Streamlit** dashboard and opens it (if reachable)

2. **Wait for Airflow init container**
   The container `airflow-airflow-init-1` will **exit** after initializing Airflow (expected). All other Airflow containers should be running.

> Prefer manual bring-up?
> `make kafka-up mongo-up airflow-up` → then `make spark-up` and `make dashboard-up`.

---

## Run Demo Data (manual first run)

1. **Load demo Delta** (inside the Airflow scheduler container)

   ```bash
   docker exec -it airflow-airflow-scheduler-1 bash
   cd /opt/project
   python3 scripts/demo/load_delta.py
   ```

2. **Analyze transcripts** (Spark batch)

   ```bash
   python3 spark/pipelines/analyze_transcripts_pipeline.py
   ```

   After success, inspect Mongo (localhost UI if available, or `make mongo-shell`).

3. **Simulate user events** (streaming)

   ```bash
   python3 scripts/streaming/user_events_simulation.py
   ```

   Wait a couple of minutes; you should see `/data/delta/user_vs_episode_daily` populated.

4. **Train similarities & compute final recommendations**

   ```bash
   python3 spark/pipelines/training_user_events_pipeline.py
   python3 spark/pipelines/final_recommendation.py
   ```

   MongoDB should now have **`final_recommendations`** populated.

After these, your **dashboard** can filter/view recommendations + live engagement windows.

---

## Ingestion & Transcription (details)

### Kafka Topics

* `episode-metadata` – full metadata for new episodes (used by downloader/transcriber, Mongo writer)
* `episode-ids` – compacted tracking topic of ids (for de-dupe / replays)
* `user-events` – user interactions (like, complete, pause, rate, skip)

### Fetch & publish metadata

`fetch_and_publish_metadata.py`:

* Fetch \~50 trending podcasts from Podcast Index
* Filter to English (metadata + language detection)
* For each feed, pick the latest episode (`title`, `author`, `description`, `audio_url`, `episode_id`)
* Publish **metadata** to `episode-metadata` (key = `episode_id`) and **id** to `episode-ids`

*Run:*

```bash
python scripts/batch/fetch_and_publish_metadata.py
```

### Transcribe (bounded micro-batch, no Spark)

`scripts/batch/new_episodes_transcript_download.py`:

* Load retryable failures (Delta `transcripts-en`, `retry_count < 3`)
* Poll Kafka for new `episode-metadata` (bounded poll)
* Upsert **episodes** metadata to Delta (so retries can discover `audio_url`)
* Process **one** episode at a time: download → transcribe → upsert transcript to Delta, commit its Kafka offset

*Run:*

```bash
python scripts/batch/new_episodes_transcript_download.py
```

> **Robustness**: commits offsets **after** idempotent writes to Delta; failures increment `retry_count` with a cap.

---

## Dashboard (Streamlit)

### What it reads

* **MongoDB**: `final_recommendations` snapshot (`user_id`, `recommended_episode_id`, `score`, `generated_at`)
* **Delta via DuckDB**: last **X** minutes of **events**, plus **episodes** metadata for titles

### Run via Makefile

```bash
make dashboard-up         # uses docker/streamlit/docker-compose.yml
make dashboard-logs
```

---

## Troubleshooting

### A) Docker can’t pull images (proxy / DNS / `http.docker.internal`)

In Docker Desktop → **Settings → Proxies**: disable or set your real proxy; **Apply & Restart**

Test:

```bash
docker pull hello-world
docker pull python:3.11-slim
```

### B) Streamlit container: “File does not exist: dashboard.py”

Your compose lives in `docker/streamlit`. Use `context: ../..` and `volumes: ../..:/app`.

Sanity:

```bash
docker compose -f docker/streamlit/docker-compose.yml exec dashboard \
  bash -lc 'ls -la /app | head -n 50'
```

### C) Port already in use (`0.0.0.0:8504`)

```bash
lsof -iTCP:8504 -sTCP:LISTEN -n -P
pkill -f 'streamlit run'    # if a local run is occupying it
# or change host port in compose: "8510:8504"
```

### D) Delta S3: “credential provider not enabled” / IMDS warnings

* For **local FS**: use filesystem paths and **unset** AWS env vars
* For **S3**: export `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_EC2_METADATA_DISABLED=true`

### E) DuckDB Delta: “No such file or directory”

Seed first:

```bash
python testing_dashboard/seed_delta_episodes.py --episodes-path ./data/delta/episodes --count 200 --mode overwrite
python testing_dashboard/seed_delta_events.py   --path ./data/delta/events    --minutes 60 --users 50 --events-per-user 10 --mode overwrite
```

---

