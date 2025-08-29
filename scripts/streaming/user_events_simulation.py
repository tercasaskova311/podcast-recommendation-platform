#!/usr/bin/env python3
import json, uuid, random, time, os, sys, hashlib
from datetime import datetime, timezone, timedelta
from typing import List, Dict
from kafka import KafkaProducer
from pymongo import MongoClient


from config.settings import (
    KAFKA_URL, TOPIC_USER_EVENTS_STREAMING,
    MONGO_URI, MONGO_DB, MONGO_COLLECTION_USERS,
    MONGO_COLLECTION_SIMILARITIES
)

EVENTS = ["pause", "like", "skip", "rate", "complete"]
MIN_USERS_PER_RUN = 30              # at least 30 users per run
BUCKET_MINUTES = 10                  # Airflow runs every 10 min -> 1 bucket

producer = KafkaProducer(
    bootstrap_servers=KAFKA_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_episode_ids(mc, limit: int = 0, sample_n: int = 500):
    coll = mc[MONGO_DB][MONGO_COLLECTION_SIMILARITIES]
    cursor = coll.find({}, {"_id": 0, "new_episode_id": 1, "historical_episode_id": 1})
    ids = []
    for d in cursor:
        if d.get("new_episode_id"):        ids.append(str(d["new_episode_id"]))
        if d.get("historical_episode_id"): ids.append(str(d["historical_episode_id"]))
    # de-dupe + drop "null"/empty
    ids = [x for x in dict.fromkeys(ids) if x and x.lower() != "null"]
    if limit > 0: ids = ids[:limit]
    if sample_n > 0 and len(ids) > sample_n: ids = random.sample(ids, sample_n)
    return ids


def floor_to_bucket(ts: datetime, minutes: int) -> datetime:
    minute = (ts.minute // minutes) * minutes
    return ts.replace(minute=minute, second=0, microsecond=0)

def bucket_id_utc(minutes: int = BUCKET_MINUTES) -> str:
    now = datetime.now(timezone.utc)
    b = floor_to_bucket(now, minutes)
    return b.isoformat()

def deterministic_event_id(user_id: str, bucket: str) -> str:
    return hashlib.sha1(f"{user_id}|{bucket}".encode("utf-8")).hexdigest()

def fetch_users(mc: MongoClient) -> List[Dict]:
    coll = mc[MONGO_DB][MONGO_COLLECTION_USERS]
    return list(coll.find({}, {"_id": 0}))

# ---- Placeholder for your future Delta check ----
def already_emitted_in_bucket(user_id: str, bucket: str) -> bool:
    # TODO: implement with Delta read
    return False

def choose_users(users: List[Dict]) -> List[Dict]:
    if not users:
        return []
    n = random.randint(MIN_USERS_PER_RUN, len(users))
    random.seed(bucket_id_utc())  # stable-ish per bucket
    return random.sample(users, n)

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

def main():
    mc = MongoClient(MONGO_URI)
    users = fetch_users(mc)
    if not users:
        print("[ERROR] No users in Mongo. Run bootstrap_users.py first.", file=sys.stderr)
        sys.exit(1)

    episode_ids = fetch_episode_ids(mc)
    if not episode_ids:
        print("[ERROR] No episode IDs found in Mongo similarities.", file=sys.stderr)
        sys.exit(2)

    bucket = bucket_id_utc()
    selected = choose_users(users)

    sent = 0
    for u in selected:
        uid = u["id"]

        if already_emitted_in_bucket(uid, bucket):
            continue

        device = random.choice(["ios", "android", "web"])
        e = random.choice(EVENTS)
        event_id = deterministic_event_id(uid, bucket)
        ep_id = random.choice(episode_ids)

        msg = {
            "event_id": event_id,
            "bucket": bucket,
            "ts": now_iso(),
            "episode_id": ep_id,
            "user_id": uid,
            "event": e,
            "device": device,
        }

        # optional extras by type
        if e == "rate":
            msg["rating"] = random.choice([1, 2, 3, 4, 5])
        elif e == "pause":
            msg["position_sec"] = random.randint(0, 3600)
        elif e == "skip":
            s = random.randint(0, 3500)
            msg["from_sec"] = s
            msg["to_sec"] = s + random.randint(5, 60)
        elif e == "complete":
            msg["played_pct"] = 1.0

        producer.send(TOPIC_USER_EVENTS_STREAMING, key=uid.encode("utf-8"), value=msg)
        sent += 1

    producer.flush()
    print(f"[INFO] Bucket {bucket}: sent {sent} events for {len(selected)} users.")
    sys.exit(0)

if __name__ == "__main__":
    main()