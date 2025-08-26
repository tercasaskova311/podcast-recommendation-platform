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

