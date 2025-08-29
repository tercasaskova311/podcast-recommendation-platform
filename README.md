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
# HOW TO RUN THE INFRASTRUCTURE

1. Run **make init** (wait until it finishes).  
   The only container that should be stopped after this command is `airflow-airflow-init-1` (it is only used to initialize Airflow).  

2. Connect to the Airflow interface at `http://localhost:8081`  
   (username: `airflow`, password: `airflow`).  
   Go to **Admin > Connections**, search for `spark_default`, click **Edit parameter**, and change the value in the **host** field to `local[*]`. Save it.  

3. Go to the **DAGs** page, run the demo DAG that initializes the system, and once it has finished, run the recommendation DAG.  

---

### LAST STEP  
After these operations, we have simulated the functionality of the system.  
To check the results, go to `http://localhost:8084`.  

