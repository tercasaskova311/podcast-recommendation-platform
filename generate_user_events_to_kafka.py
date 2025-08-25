# scripts/generate_user_events_to_kafka.py
import os, json, uuid, random, sys
from datetime import datetime, timezone
from typing import List
from faker import Faker
from confluent_kafka import Producer
from pymongo import MongoClient

from spark.config.settings import (
    KAFKA_URL, TOPIC_USER_EVENTS_STREAMING,
    MONGO_URI, MONGO_DB, MONGO_COLLECTION, EPISODE_ID_FIELD,
    NUM_USERS, MIN_EPS, MAX_EPS,
    EPISODE_LIMIT, EPISODE_SAMPLE_N,
)

# Allow quick env overrides, but default to settings.py
BROKER = os.getenv("KAFKA_URL", KAFKA_URL)
TOPIC  = os.getenv("TOPIC_USER_EVENTS_STREAMING", TOPIC_USER_EVENTS_STREAMING)

EP_COLL  = MONGO_COLLECTION
EP_FIELD = EPISODE_ID_FIELD


# ---------- Globals ----------
fake = Faker()
p = Producer({"bootstrap.servers": BROKER})

EVENTS = ["pause","like","skip","rate","complete"]

# ---------- Helpers ----------
def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def fetch_episode_ids_from_mongo(limit: int = 0, sample_n: int = 0) -> List[str]:
    """
    Fetch all episode_ids from Mongo (no filtering).
    - limit: cap the number of IDs pulled from Mongo (0 = no limit)
    - sample_n: randomly sample N IDs client-side after fetch (0 = no sampling)
    """
    try:
        mc = MongoClient(MONGO_URI)
        coll = mc[MONGO_DB][EP_COLL]            # EP_COLL is the collection name string
        proj = {EP_FIELD: 1, "_id": 0}          # EP_FIELD like "episode_id"

        cursor = coll.find({}, proj)            # no filtering
        if limit > 0:
            cursor = cursor.limit(limit)

        ids = [str(doc[EP_FIELD]) for doc in cursor if EP_FIELD in doc]
        ids = list(dict.fromkeys(ids))          # de-dupe, keep order

        if sample_n > 0 and len(ids) > sample_n:
            ids = random.sample(ids, sample_n)  # population, k

        return ids
    except Exception as e:
        print(f"[WARN] Mongo fetch failed: {e}")
        return []


def generate_events(episodes: List[str], num_users=300, min_eps=1, max_eps=5) -> None:
    if not episodes:
        print("[ERROR] No episode_ids available to generate events.")
        sys.exit(1)

    user_ids = [str(uuid.uuid4()) for _ in range(num_users)]

    for uid in user_ids:
        device = random.choice(["ios","android","web"])
        n = random.randint(min_eps, max_eps)
        for _ in range(n):
            ep = random.choice(episodes)
            e  = random.choice(EVENTS)

            msg = {
                "event_id": str(uuid.uuid4()),
                "ts": now_utc(),                # ISO8601
                "user_id": uid,
                "episode_id": ep,
                "event": e,                     # one action per message
                "device": device
            }
            if e == "rate":
                msg["rating"] = random.choice([1,2,3,4,5])
            elif e == "pause":
                msg["position_sec"] = random.randint(0, 3600)
            elif e == "skip":
                s = random.randint(0, 3500)
                msg["from_sec"] = s
                msg["to_sec"]   = s + random.randint(5, 60)
            elif e == "complete":
                msg["played_pct"] = 1.0

            # key by user for per-user ordering
            p.produce(TOPIC, key=uid, value=json.dumps(msg).encode("utf-8"))

    p.flush()
    print(f"[INFO] Sent events for {len(user_ids)} users over {len(episodes)} episodes.")

# ---------- Main ----------
if __name__ == "__main__":
    eps = fetch_episode_ids_from_mongo()
    generate_events(eps, NUM_USERS, MIN_EPS, MAX_EPS)
