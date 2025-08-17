import json
import os
import sys
from kafka import KafkaProducer

KAFKA_URL = os.getenv("KAFKA_URL")
TOPIC_EPISODES_ID = os.getenv("TOPIC_EPISODES_ID")
TOPIC_EPISODE_METADATA = os.getenv("TOPIC_EPISODE_METADATA")
SAMPLE_EPISODES_JSON_PATH = os.getenv("SAMPLE_EPISODES_JSON_PATH")


def main():

    if not os.path.exists(SAMPLE_EPISODES_JSON_PATH):
        print(f"ERROR: file not found: {SAMPLE_EPISODES_JSON_PATH}", file=sys.stderr)
        sys.exit(1)

    with open(SAMPLE_EPISODES_JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
        if not isinstance(data, list):
            print("ERROR: JSON must be an array of episode objects.", file=sys.stderr)
            sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        linger_ms=10,
        acks="all"
    )

    sent = 0
    for rec in data:
        key = rec.get("episode_id")
        
        # Send metadata
        producer.send(
            TOPIC_EPISODE_METADATA,
            key=str(key).encode('utf-8') if key else None,
            value=rec
        )

        # Send ID to tracking topic
        producer.send(
            TOPIC_EPISODES_ID,
            key=str(key).encode('utf-8') if key else None,
            value=str(key)
        )

        sent += 1

    producer.flush()
    print(f"Seeded {sent} records to topic '{TOPIC_EPISODE_METADATA}' at {KAFKA_URL}")

if __name__ == "__main__":
    main()
