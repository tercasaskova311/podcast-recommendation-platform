import requests
import hashlib
import time
import json
import os
from kafka import KafkaProducer
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

# --- CONFIG ---
API_KEY = "4YCF385ZNXLEHADRUSFV"
API_SECRET = "YcjAfd9x7S$$qX^4u#n$rvLD$X6c^pT87ShWvF3V"
KAFKA_URL = "kafka1:9092"
TOPIC_RAW_PODCAST = os.getenv("TOPIC_RAW_PODCAST", "raw-podcast-metadata")

# ====== AUTH HEADERS FOR PODCAST INDEX ======
timestamp = str(int(time.time()))
auth_hash = hashlib.sha1((API_KEY + API_SECRET + timestamp).encode('utf-8')).hexdigest()

headers = {
    "X-Auth-Date": timestamp,
    "X-Auth-Key": API_KEY,
    "Authorization": auth_hash,
    "User-Agent": "PodcastIndex-Client"
}

# ====== CHECK LANGUAGE ======
def is_english(feed):
    if feed.get("language", "").lower() == "en":
        return True
    try:
        text = f"{feed.get('title', '')} {feed.get('description', '')}"
        return detect(text) == "en"
    except LangDetectException:
        return False

# ====== FETCH PODCAST METADATA ======
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

# ====== KAFKA PRODUCER ======
def publish_to_kafka(episodes):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_URL,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for ep in episodes:
        print(f" Sending '{ep['episode_title']}' to Kafka...")
        producer.send(TOPIC_RAW_PODCAST, ep)

    producer.flush()
    producer.close()
    print(" All episodes sent to Kafka.")

# ====== MAIN ======
if __name__ == "__main__":
    episodes = fetch_episodes()

    if not episodes:
        print(" No episodes found.")
        exit()

    # Optional: save a local copy
    with open("episodes.json", "w") as f:
        json.dump(episodes, f, indent=4)

    publish_to_kafka(episodes)
