
 **user_events NOTES** 
* **EXPECTED OUTPUT: Personalized Top-N Recommendations** → “Recommended for You”

---
## ** Architecture**

```
1) Event Generation (Batch)
2) Ingestion & Aggregation (Streaming)
3) ALS Training & Scoring (Batch)
```

### **Flow**

```text
Kafka (raw events) based on mongo from transcritps 
    ↓
Structured Streaming
    ↓ parse + dedupe + weight
    ↓
Delta (daily engagement: user_id × episode_id × day)
    ↓
ALS Training (aggregates across days)
    ↓
Recommendations in MongoDB
```

---

## ** Scripts**

### **1️⃣ Event Generation — `generate_user_events_to_kafka.py`**

**Purpose:**
Generates **synthetic user events** and pushes them to **Kafka** for testing.

**Features:**

* Uses `Faker` to simulate users and podcast episodes.
* Sends events like: `like`, `pause`, `skip`, `rate`, `complete`.
* Produces JSON messages keyed by `user_id` for ordering.

**Example event:**

```json
{
  "event_id": "abcd-1234",
  "ts": "2025-08-25T12:34:56Z",
  "user_id": "U1",
  "new_episode_id": "E42",
  "event": "like",
  "device": "ios"
}
```

---

### **2️⃣ Ingestion & Aggregation — `ingest_user_events_stream.py`**

**Purpose:**
Consumes events from **Kafka**, processes them using **Spark Structured Streaming**, and writes **daily engagement** into a **Delta Lake table**.

**Main Steps:**

1. **Read from Kafka** → real-time event stream.
2. **Parse JSON** & **deduplicate** (`event_id`).
3. **Assign weights** to events:

   * ✅ `like` → positive boost
   * ✅ `complete` → strong positive
   * ✅ `skip` → negative
   * ✅ `pause` → scaled by listened seconds
   * ✅ `rate` → rating-based adjustment
4. **Aggregate engagement daily** per **(user, episode, day)**.
5. **Upsert into Delta Lake** (`DELTA_PATH_DAILY`).

**Output (Silver Table):**

| user\_id | new\_episode\_id | day        | engagement | num\_events | last\_ts         |
| -------- | ---------------- | ---------- | ---------- | ----------- | ---------------- |
| U1       | E42              | 2025-08-25 | 4.5        | 3           | 2025-08-25 11:33 |
| U2       | E99              | 2025-08-25 | 2.0        | 1           | 2025-08-25 10:05 |

---

### **3️⃣ ALS Training & Scoring — `train_ALS.py`**

**Purpose:**
Trains an **ALS implicit feedback model** on aggregated engagement data and stores recommendations in **MongoDB**.

**Main Steps:**

1. **Load Silver Table** from `DELTA_PATH_DAILY`.
2. **Aggregate engagement** across **all days** per `(user, episode)`.
3. **Index string IDs** → numeric indices (`user_idx`, `item_idx`).
4. **Train ALS**:

   * Implicit feedback (`implicitPrefs=True`).
   * Uses weighted engagement as **confidence scores**.
5. **Generate Recommendations**:

   * **Personalized Top-N per User** → `als_top_n` collection.

---

## ** Outputs**

### **1. Personalized Recommendations — `als_top_n`**

> **“Recommended for You”**

| user\_id | new\_episode\_id | als\_score |
| -------- | ---------------- | ---------- |
| U1       | E42              | 0.95       |
| U1       | E11              | 0.92       |
| U2       | E99              | 0.88       |

---

## ** How to Run**

### **Step 1. Start Kafka**

```bash
kafka-server-start.sh config/server.properties
```

### **Step 2. Generate Events**

```bash
python scripts/generate_user_events_to_kafka.py
```

Verify with:

```bash
kafka-console-consumer \
  --bootstrap-server <broker_url> \
  --topic <TOPIC_USER_EVENTS_STREAMING> \
  --from-beginning
```

---

### **Step 3. Run Ingestion Job**

```bash
python spark/jobs/ingest_user_events_stream.py
```

> Keep running until you see data in `DELTA_PATH_DAILY`.

---

### **Step 4. Train ALS & Store Recommendations**

```bash
python spark/jobs/train_ALS.py
```

Check MongoDB:

```bash
mongosh
use recommendations
db.als_top_n.find().limit(3).pretty()
db.als_item_item.find().limit(3).pretty()
```
---

## ** Summary Diagram**

```
generate_user_events_to_kafka.py
    ↓ Kafka (stream of events)
ingest_user_events_stream.py
    ↓ Parse → Deduplicate → Weight → Aggregate
    ↓ Delta Table (daily engagement)
train_ALS.py
    ↓ Aggregate across days
    ↓ Train ALS (implicit feedback)
    ↓
Top-N recs → MongoDB ("als_top_n")
```

---

