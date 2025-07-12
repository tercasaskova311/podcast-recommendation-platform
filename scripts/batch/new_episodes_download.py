import requests
import hashlib
import time
import json
import os
from kafka import KafkaProducer, KafkaConsumer
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

# Env vars
KAFKA_URL = os.getenv("KAFKA_URL")
TOPIC_EPISODES_ID = os.getenv("TOPIC_EPISODES_ID")
TOPIC_EPISODE_METADATA = os.getenv("TOPIC_EPISODE_METADATA")
API_PODCAST_INDEX_KEY = os.getenv("API_PODCAST_INDEX_KEY")
API_PODCAST_INDEX_SECRET= os.getenv("API_PODCAST_INDEX_SECRET")
GROUP_ID = "metadata-discoverer-reader"

# ====== AUTH HEADERS FOR PODCAST INDEX ======
timestamp = str(int(time.time()))
auth_hash = hashlib.sha1((API_PODCAST_INDEX_KEY + API_PODCAST_INDEX_SECRET + timestamp).encode('utf-8')).hexdigest()

headers = {
    "X-Auth-Date": timestamp,
    "X-Auth-Key": API_PODCAST_INDEX_KEY,
    "Authorization": auth_hash,
    "User-Agent": "PodcastIndex-Client"
}

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def get_known_episode_ids(timeout=5):
# Initialize Kafka consumer to read existing episode IDs
    consumer = KafkaConsumer(
        TOPIC_EPISODES_ID,
        bootstrap_servers=KAFKA_URL,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset='earliest'
    )

    known_ids = set()
    start = time.time()

    while True:
        records = consumer.poll(timeout_ms=500)
        if not records:
            if time.time() - start > timeout:
                break  # No more messages and timeout expired
            continue  # No data yet, try again briefly

        for tp_records in records.values():
            for msg in tp_records:
                if msg.key:
                    known_ids.add(msg.key)

    consumer.close()
    return known_ids

# Check language
def is_english(feed):
    if feed.get("language", "").lower() == "en":
        return True
    try:
        text = f"{feed.get('title', '')} {feed.get('description', '')}"
        return detect(text) == "en"
    except LangDetectException:
        return False
    
# Fetch latest episodes from external API
def fetch_episodes():
    url = "https://api.podcastindex.org/api/1.0/podcasts/trending"
    response = requests.get(url, headers=headers, params={"max": 50})

    if response.status_code != 200:
        print("Failed to retrieve trending podcasts")
        exit()

    feeds = response.json().get("feeds", [])
    english_feeds = [feed for feed in feeds if is_english(feed)]
    print(f"{len(english_feeds)} English podcasts found.")

    episodes = []
    for feed in english_feeds:
        feed_id = feed.get("id")
        podcast_title = feed.get("title")

        print(f" Fetching episode for: {podcast_title}")
        res = requests.get(
            "https://api.podcastindex.org/api/1.0/episodes/byfeedid",
            headers=headers,
            params={"id": feed_id, "max": 1}
        )
        if res.status_code != 200:
            continue

        for ep in res.json().get("items", []):
            audio_url = ep.get("enclosureUrl")
            if audio_url:
                episodes.append({
                    "podcast_title": podcast_title,
                    "podcast_author": feed.get("author"),
                    "podcast_url": feed.get("url"),
                    "episode_title": ep.get("title"),
                    "description": ep.get("description"),
                    "audio_url": audio_url,
                    "episode_id": ep.get("id")
                })

    return episodes

# Main logic
def discover_and_publish_new_episodes():
    known_ids = get_known_episode_ids()
    latest_episodes = fetch_episodes()

    new_episodes = [ep for ep in latest_episodes if ep["episode_id"] not in known_ids]

    for episode in new_episodes:
        episode_id = episode["episode_id"]
        
        # Send metadata
        producer.send(
            TOPIC_EPISODE_METADATA,
            key=str(episode_id).encode('utf-8') if episode_id else None,
            value=episode
        )

        # Send ID to tracking topic
        producer.send(
            TOPIC_EPISODES_ID,
            key=str(episode_id).encode('utf-8') if episode_id else None,
            value=str(episode_id)
        )

        print(f"Published new episode: {episode_id}")

    producer.flush()
    print(f"Finished: {len(new_episodes)} new episodes published.")

if __name__ == "__main__":
    discover_and_publish_new_episodes()