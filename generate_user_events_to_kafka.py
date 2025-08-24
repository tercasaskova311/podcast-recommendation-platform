# generate_user_events_to_kafka.py
import os, json, uuid, random
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import Producer


BROKER = os.getenv("KAFKA_URL", "kafka1:9092")
TOPIC  = os.getenv("TOPIC_USER_EVENTS_STREAMING", "user-events-streaming")

fake = Faker()
p = Producer({"bootstrap.servers": BROKER})

EVENTS = ["pause","like","skip","rate","complete"]
RATING = [1,2,3,4,5]

def now_utc():
    return datetime.now(timezone.utc).isoformat()

def send(evt: dict):
    p.produce(TOPIC, key=evt["user_id"], value=json.dumps(evt).encode("utf-8"))

def main(episodes: list[str], num_users=300, min_eps=1, max_eps=5):
    user_ids = [str(uuid.uuid4()) for _ in range(num_users)]
    for uid in user_ids:
        device = random.choice(["ios","android","web"])
        for _ in range(random.randint(min_eps, max_eps)):
            ep = random.choice(episodes)
            e  = random.choice(EVENTS)
            send({
                "event_id": str(uuid.uuid4()),
                "user_id": uid,
                "episode_id": ep,
                "event": e,
                "rating": random.choice(RATING) if e=="rate" else None,
                "device": device,
                "ts": now_utc()
            })
    p.flush()

if __name__ == "__main__":
    with open("top_episodes.json") as f:
        top_episodes = [x["id"] for x in json.load(f)]
    main(top_episodes)
