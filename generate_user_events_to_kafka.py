# generate_user_events_to_kafka.py
import os, json, uuid, random, sys
from datetime import datetime, timezone
from typing import List
from faker import Faker
from confluent_kafka import Producer
from pymongo import MongoClient

# ---------- Config (env) ----------
BROKER = os.getenv("KAFKA_URL", "kafka1:9092")
TOPIC  = os.getenv("TOPIC_USER_EVENTS_STREAMING", "user-events-streaming")

MONGO_URI   = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB    = os.getenv("MONGO_DB", "podcasts")
EP_COLL     = os.getenv("EPISODES_COLLECTION", "episodes")
EP_FIELD    = os.getenv("EPISODE_ID_FIELD", "episode_id")

EP_MATCH_JSON = os.getenv("EPISODE_MATCH", "")  # e.g. '{"language":"en"}'
EP_LIMIT      = int(os.getenv("EPISODE_LIMIT", "0"))
EP_SAMPLE_N   = int(os.getenv("EPISODE_SAMPLE_N", "0"))

NUM_USERS     = int(os.getenv("NUM_USERS", "300"))
MIN_EPS       = int(os.getenv("MIN_EPS_PER_USER", "1"))
MAX_EPS       = int(os.getenv("MAX_EPS_PER_USER", "5"))

# ---------- Globals ----------
fake = Faker()
p = Producer({"bootstrap.servers": BROKER})

EVENTS = ["pause","like","skip","rate","complete"]

# ---------- Helpers ----------
def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def fetch_episode_ids_from_mongo() -> List[str]:
    try:
        mc = MongoClient(MONGO_URI)
        coll = mc[MONGO_DB][EP_COLL]
        match = json.loads(EP_MATCH_JSON) if EP_MATCH_JSON.strip() else {}
        proj  = {EP_FIELD: 1, "_id": 0}

        cursor = coll.find(match, proj)
        if EP_LIMIT > 0:
            cursor = cursor.limit(EP_LIMIT)

        ids = [str(doc[EP_FIELD]) for doc in cursor if EP_FIELD in doc]
        ids = list(dict.fromkeys(ids))  # dedupe preserve order

        if EP_SAMPLE_N > 0 and len(ids) > EP_SAMPLE_N:
            ids = random.sample(ids, EP_SAMPLE_N)

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
