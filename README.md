# Podcast Analytics & Recommendation Platform

This project builds an end-to-end data pipeline to recommend podcast episodes to users based on their interactions and also podcasts content. The workflow includes:

- Downloading new podcast episodes
- Analyzing transcripts
- Tracking user behavior (events)
- Training recommendation models
- Delivering real-time personalized recommendation for a podcasts episode

---
## Project Structure

root/
├── _delta/ # Delta Lake tables (episodes, events)
├── airflow/ # Apache Airflow config for DAG orchestration
├── config/
│ └── settings.py # General project settings
├── data/
├── docker/ # Docker-related configuration
├── docs/ # Documentation & diagrams
├── scripts/
├── spark/ # Spark pipelines for large-scale processing
│ ├── pipelines/
├── demo/ # Demos or example runs
├── dashboard.py # Interactive dashboard
├── .env.development # Environment variables
├── .gitignore
├── Makefile # Dev shortcuts (build, test, run)
├── requirements.txt
└── README.md

---

## Pipeline Overview

| Stage                        | Description                                                                |
|------------------------------|----------------------------------------------------------------------------|
|  Ingestion                   | Download new episodes, fetch transcripts                                   |
|  Transcript Analysis         | NLP-based feature extraction using Spark                                   |
|  User Events Streaming       | Simulation of Real-time interaction data (views, clicks, time spent)       |
|  Model Training              | Collaborative filtering + content-based recommendation models              |
|  Final Recommendation        | Scores and recommends episodes                                             |
|  Dashboard                   | Interactive UI to explore recommendations and model metrics                |

---
##  Technologies

| Area            | Tools Used                           |
|-----------------|--------------------------------------|
| Workflow        | Apache Airflow                       |
| Storage         | Delta Lake (with `_delta` tables),   |
|                 | MongoDB, DuckDB                      |
| Processing      | Apache Spark, Kafka                  |
| Containerization| Docker                               |
| NLP             | Transformers / Text Processing       |
| Real-time       | Spark Streaming                      |
| UI              | Streamlit                            |

---

## Architecture

## Architecture Diagram

![Architecture Diagram](./docs/project_architecture.png)

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
