# scripts/generate_user_events_to_kafka.py
import json, uuid, random, sys
from datetime import datetime, timezone
from typing import List
from faker import Faker
from kafka import KafkaProducer
from pymongo import MongoClient
import time
import os

from config.settings import (
    KAFKA_URL, TOPIC_USER_EVENTS_STREAMING, 
    MONGO_URI, MONGO_DB, MONGO_COLLECTION_SIMILARITIES,
    NUM_USERS, MIN_EPS_PER_USER, MAX_EPS_PER_USER,
    EPISODE_LIMIT, EPISODE_SAMPLE_N,

)

WINDOW_SEC = 300  # 5 minutes default
JITTER = 0.5  # 50% jitter around average delay
EP_COLL  = MONGO_COLLECTION_SIMILARITIES
EP_FIELD = "new_episode_id"


# ---------- Globals ----------
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=KAFKA_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

EVENTS = ["pause","like","skip","rate","complete"]

# ---------- Helpers ----------
def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def fetch_episode_ids_from_mongo(limit: int = EPISODE_LIMIT, sample_n: int = EPISODE_SAMPLE_N) -> List[str]:
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

def generate_events(
    episodes: List[str],
    num_users: int = NUM_USERS,
    min_eps: int = MIN_EPS_PER_USER,
    max_eps: int = MAX_EPS_PER_USER,
    window_sec: int = WINDOW_SEC,
) -> None:
    if not episodes:
        print("[ERROR] No episode_ids available to generate events.")
        sys.exit(1)

    # --- plan counts first so pacing is accurate ---
    user_ids = [str(uuid.uuid4()) for _ in range(num_users)]
    events_per_user = {uid: random.randint(min_eps, max_eps) for uid in user_ids}
    total_events = sum(events_per_user.values())
    if total_events <= 0:
        print("[WARN] No events planned (min/max too low?).")
        return

    avg_delay = float(window_sec) / total_events  # average spacing between events

    for uid in user_ids:
        device = random.choice(["ios", "android", "web"])
        n = events_per_user[uid]

        for _ in range(n):
            ep = random.choice(episodes)
            e  = random.choice(EVENTS)

            msg = {
                "event_id": str(uuid.uuid4()),
                "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "user_id": uid,
                "episode_id": ep,
                "event": e,
                "device": device,
            }
            if e == "rate":
                msg["rating"] = random.choice([1, 2, 3, 4, 5])
            elif e == "pause":
                msg["position_sec"] = random.randint(0, 3600)
            elif e == "skip":
                s = random.randint(0, 3500)
                msg["from_sec"] = s
                msg["to_sec"]   = s + random.randint(5, 60)
            elif e == "complete":
                msg["played_pct"] = 1.0

            # key by user for per-user ordering; pace with jitter
            producer.send(TOPIC_USER_EVENTS_STREAMING, key=uid.encode("utf-8"), value=msg)

            # jittered spacing so the stream doesn't look perfectly regular
            sleep_s = random.uniform((1 - JITTER) * avg_delay, (1 + JITTER) * avg_delay)
            if sleep_s > 0:
                time.sleep(sleep_s)

    producer.flush()
    print(f"[INFO] Sent {total_events} events for {len(user_ids)} users over {len(episodes)} episodes in ~{window_sec}s.")


# ---------- Main ----------
if __name__ == "__main__":
    eps = fetch_episode_ids_from_mongo()
    generate_events(eps, NUM_USERS, MIN_EPS_PER_USER, MAX_EPS_PER_USER)
