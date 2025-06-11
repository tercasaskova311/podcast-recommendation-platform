import json
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

# Load the .env file
load_dotenv('/opt/airflow/.env.development')

# Now use the environment variables
KAFKA_URL = os.getenv("KAFKA_URL")
TOPIC_RAW_PODCAST = os.getenv("TOPIC_RAW_PODCAST")

producer = KafkaProducer(bootstrap_servers=KAFKA_URL, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

with open('./scripts/demo/bootstrap_transcriptions.json') as f:
    demo_transcriptions = json.load(f)

for episode in demo_transcriptions:
    print(episode)
    producer.send(TOPIC_RAW_PODCAST, episode)
