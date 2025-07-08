import json
import os
from kafka import KafkaProducer

# Now use the environment variables
KAFKA_URL = os.getenv("KAFKA_URL")
TOPIC_RAW_PODCAST = os.getenv("TOPIC_RAW_PODCAST")

producer = KafkaProducer(bootstrap_servers=KAFKA_URL, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

with open('/opt/scripts/demo/bootstrap_transcriptions.json') as f:
    demo_transcriptions = json.load(f)

for episode in demo_transcriptions:
    print(episode)
    producer.send(TOPIC_RAW_PODCAST, episode)

producer.flush(timeout=10)  # fino a 10 secondi di attesa
producer.close()
