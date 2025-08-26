# User Events Notes

-  **generation** (Kafka)
- **streaming / aggregation** (Spark → Delta)
- **training a recommendations** (ALS → Mongo)

---

## 1. Generating events (`spark/pipelines/generate_user_events_pipeline.py`)

| **Component**       | **Purpose & Data Flow**                                                                                                                                                                                                                  |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Input source**    | MongoDB (collection of podcast episode IDs).                                                                                                                                                                                             |
| **Helper**          | `fetch_episode_ids_from_mongo()` → pulls `new_episode_id` values. Can limit or sample.                                                                                                                                                       |
| **Synthetic users** | `uuid4()` generates `NUM_USERS` random IDs.                                                                                                                                                                                              |
| **Event types**     | One of `["pause","like","skip","rate","complete"]`. Each has extra metadata: <br> - `rate` → adds `rating` 1–5 <br> - `pause` → adds `position_sec` <br> - `skip` → adds `from_sec`, `to_sec` <br> - `complete` → adds `played_pct=1.0`. |
| **Message format**  | JSON with fields: `event_id`, `ts`, `user_id`, `new_episode_id`, `event`, `device`, plus event-specific fields.                                                                                                                              |
| **Output sink**     | Kafka (`TOPIC_USER_EVENTS_STREAMING`), keyed by `user_id` (ensures per-user ordering).                                                                                                                                                   |
| **Storage**         | Not persisted locally; only streamed into Kafka.                                                                                                                                                                                         |

---

## 2. Spark Streaming (User Events → Delta)

| **Component**                   | **Purpose & Data Flow**                                                                                                                                                                            |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Input source**                | Kafka topic (`TOPIC_USER_EVENTS_STREAMING`).                                                                                                                                                       |
| **Schema**                      | JSON fields: `event_id, user_id, new_episode_id, event, rating, device, ts, position_sec, from_sec, to_sec, played_pct`.                                                                           |
| **Parsing**                     | `CAST(value AS STRING)` → `from_json(schema)`.                                                                                                                                                     |
| **Deduplication**               | `.withWatermark("ts","2 hours").dropDuplicates(["event_id"])`.                                                                                                                                     |
| **Weighting events**            | Map events to engagement scores: <br> - like → `LIKE_W` <br> - complete → `COMPLETE_W` <br> - rate → `(rating - 3.0)` <br> - pause → `min(position_sec/PAUSE_SEC_CAP,1.0)` <br> - skip → `SKIP_W`. |
| **Aggregation per micro-batch** | Group by `(user_id,new_episode_id,day)` → `sum(weight)` as `engagement`, `count(*)` as `num_events`, `max(ts)` as `last_ts`.                                                                       |
| **Storage**                     | Delta Lake table at `DELTA_PATH_DAILY` (Silver). Merge/upsert ensures incremental updates.                                                                                                         |
| **Checkpointing**               | Checkpoint dir = `USER_EVENT_STREAM`.                                                                                                                                                              |

---

## 3. Training (Spark ML: ALS - Alternating Leaast Square)

| **Component**             | **Purpose & Data Flow**                                                                                                                                                                                                                |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Input source**          | Delta table (`DELTA_PATH_DAILY`).                                                                                                                                                                                                      |
| **Pre-processing**        | Aggregate daily rows into total per `(user_id, new_episode_id)`. Filter `engagement > MIN_ENGAGEMENT` (removes noise).                                                                                                                 |
| **ID Encoding**           | Spark ML `StringIndexer`: <br> - `user_id → user_idx` <br> - `new_episode_id → item_idx`. Save both mappings for decoding.                                                                                                             |
| **ALS model**             | `ALS` collaborative filtering (implicit feedback mode): <br> - `userCol="user_idx"` <br> - `itemCol="item_idx"` <br> - `ratingCol="engagement"` <br> - `rank=ALS_RANK`, `regParam=ALS_REG`, `maxIter=ALS_MAX_ITER`, `alpha=ALS_ALPHA`. |
| **Training**              | Learns latent factors for users + episodes → predicts preference strength.                                                                                                                                                             |
| **Output**                | - ALS model saved to `ALS_MODEL_PATH`. <br> - Indexers saved alongside model.                                                                                                                                                          |
| **Top-N recommendations** | `recommendForAllUsers(TOP_N)` → explode into rows `(user_id, new_episode_id, als_score)`.                                                                                                                                              |
| **Final storage**         | MongoDB (`MONGO_DB_USER_EVENTS`.`MONGO_COLLECTION_USER_EVENTS`).                                                                                                                                                                       |

---

## Data Flow Summary

```mermaid
flowchart LR
  A[MongoDB (Episodes)] -->|episode_ids| B[Event Generator]
  B -->|synthetic JSON events| C[Kafka Topic]
  C --> D[Spark Streaming]
  D -->|weights + agg| E[Delta Table]
  E -->|training data| F[ALS Model Training]
  F -->|Top-N recs| G[MongoDB (User Recs)]
```

---

##  ALS Training Explained

* **Goal:** Learn hidden “dimensions” that explain which users like which episodes.
* **Input matrix:** Rows = users, columns = episodes, entries = engagement scores

* **ALS (Alternating Least Squares):**

  1. Start with random latent factors for users & items.
  2. Fix user factors → solve least squares for items.
  3. Fix item factors → solve least squares for users.
  4. Alternate until convergence (minimizes error).
* **Why implicitPrefs=True?**
  Engagement is *observed behavior* (pause, like, etc.), not explicit ratings. ALS treats them as confidence-weighted implicit feedback.
* **Outputs:** For each user, vector in latent space. For each episode, vector in same space. Dot product = predicted affinity.
* **Recommendations:** Rank episodes by predicted affinity per user.

---

## Storage Overview

| **Stage**                              | **Storage**                           |
| -------------------------------------- | ------------------------------------- |
| Episode catalog                        | MongoDB (source of episode\_ids).     |
| User events (raw)                      | Kafka topic.                          |
| User events (Silver, daily engagement) | Delta Lake at `DELTA_PATH_DAILY`.     |
| Checkpoints                            | Filesystem dir = `USER_EVENT_STREAM`. |
| ALS model + indexers                   | Filesystem dir = `ALS_MODEL_PATH`.    |
| Recommendations                        | MongoDB (`MONGO_DB_USER_EVENTS`).     |

---