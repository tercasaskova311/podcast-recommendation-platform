#!/usr/bin/env python3
import json, uuid, random, time, os, sys, hashlib
from datetime import datetime, timezone, timedelta
from typing import List, Dict
from kafka import KafkaProducer
from pymongo import MongoClient


from config.settings import (
    KAFKA_URL, TOPIC_USER_EVENTS_STREAMING,
    MONGO_URI, MONGO_DB, MONGO_COLLECTION_USERS
)

EVENTS = ["pause", "like", "skip", "rate", "complete"]
MIN_USERS_PER_RUN = 30              # at least 100 users per run
BUCKET_MINUTES = 10                  # Airflow runs every 10 min -> 1 bucket

producer = KafkaProducer(
    bootstrap_servers=KAFKA_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

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

        msg = {
            "event_id": event_id,
            "bucket": bucket,
            "ts": now_iso(),
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