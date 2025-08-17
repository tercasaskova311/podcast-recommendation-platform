#MATTEO CODE

#I am just adding here something in order to test and progress with saprk part - change for mateo code later... 

import json
import os
import random
import time
from kafka import KafkaProducer

# ==== CONFIG ====
KAFKA_BROKER = "localhost:9092"
TOPIC = "streaming"

# ==== FAKE DATA POOLS ====
user_ids = list(range(1, 11))           # 10 fake users
episode_ids = list(range(1000, 1020))   # 20 fake episodes

# ==== Kafka Producer ====
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: v.encode("utf-8")
)

# ==== Infinite data stream ====
while True:
    user_id = random.choice(user_ids)
    episode_id = random.choice(episode_ids)

    likes = random.randint(0, 1)
    completions = random.randint(0, 1)
    skips = random.randint(0, 1)

    # Format: user_id,episode_id,likes,completions,skips
    csv_event = f"{user_id},{episode_id},{likes},{completions},{skips}"
    producer.send(TOPIC, value=csv_event)

    print(f"Sent: {csv_event}")
    time.sleep(0.5)  # Send a message every 0.5 seconds
