import re
import requests
import hashlib
import time
import json
import os
from kafka import KafkaProducer, KafkaConsumer
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException
from config.settings import API_PODCAST_INDEX_KEY, API_PODCAST_INDEX_SECRET, KAFKA_URL, TOPIC_EPISODES_ID, TOPIC_EPISODE_METADATA

GROUP_ID = "metadata-discoverer-reader"

def _normalize_id(x):
    """
    Return a clean string id consisting of digits only.
    Handles ints, strings, and bad historical values like "b'12345'".
    """
    if x is None:
        return None
    s = str(x)
    # extract the first run of digits
    m = re.search(r"\d+", s)
    return m.group(0) if m else s

def get_known_episode_ids(timeout=5):
    """
    Read ALL existing ids from the tracking topic (earliest), normalize them, and return as a set of strings.
    """
    consumer = KafkaConsumer(
        TOPIC_EPISODES_ID,
        bootstrap_servers=KAFKA_URL,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        value_deserializer=lambda v: v.decode("utf-8") if v else None,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset='earliest'
    )

    known_ids = set()
    start = time.time()

    while True:
        records = consumer.poll(timeout_ms=500)
        if not records:
            if time.time() - start > timeout:
                break
            continue

        for tp_records in records.values():
            for msg in tp_records:
                kid = _normalize_id(msg.key)
                if kid:
                    known_ids.add(kid)

    consumer.close()
    return known_ids

def _headers_for_podcast_index():
    timestamp = str(int(time.time()))
    auth_hash = hashlib.sha1(
        (API_PODCAST_INDEX_KEY + API_PODCAST_INDEX_SECRET + timestamp).encode('utf-8')
    ).hexdigest()
    return {
        "X-Auth-Date": timestamp,
        "X-Auth-Key": API_PODCAST_INDEX_KEY,
        "Authorization": auth_hash,
        "User-Agent": "PodcastIndex-Client"
    }

def is_english(feed):
    if feed.get("language", "").lower() == "en":
        return True
    try:
        text = f"{feed.get('title', '')} {feed.get('description', '')}"
        return detect(text) == "en"
    except LangDetectException:
        return False

def fetch_episodes():
    headers = _headers_for_podcast_index()
    url = "https://api.podcastindex.org/api/1.0/podcasts/trending"
    response = requests.get(url, headers=headers, params={"max": 50})
    response.raise_for_status()

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
            if not audio_url:
                continue
            episodes.append({
                "podcast_title": podcast_title,
                "podcast_author": feed.get("author"),
                "podcast_url": feed.get("url"),
                "episode_title": ep.get("title"),
                "description": ep.get("description"),
                "audio_url": audio_url,
                "episode_id": ep.get("id")   # keep as given; we’ll normalize when filtering
            })

    return episodes

# Producer is defined once to reuse TCP connection
producer = KafkaProducer(
    bootstrap_servers=KAFKA_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def discover_and_publish_new_episodes():
    known_ids = get_known_episode_ids()
    print(f"Already processed ids: {len(known_ids)}")

    latest_episodes = fetch_episodes()
    print(f"Downloaded episodes: {len(latest_episodes)}")

    # Deduplicate within this batch (some feeds might collide)
    seen_in_batch = set()
    deduped = []
    for ep in latest_episodes:
        eid_str = _normalize_id(ep.get("episode_id"))
        if not eid_str:
            continue
        if eid_str in seen_in_batch:
            continue
        seen_in_batch.add(eid_str)
        deduped.append(ep)

    # Keep only truly new ones, comparing normalized string ids
    new_episodes = []
    for ep in deduped:
        eid_str = _normalize_id(ep.get("episode_id"))
        if eid_str not in known_ids:
            new_episodes.append(ep)
            # pessimistically add to known set to avoid double-send within same run
            known_ids.add(eid_str)

    print(f"We have {len(new_episodes)} new espisodes")

    for episode in new_episodes:
        eid_str = _normalize_id(episode["episode_id"])

        # Send metadata (full object) – key must be a string
        producer.send(
            TOPIC_EPISODE_METADATA,
            key=eid_str.encode("utf-8") if eid_str else None,
            value=episode
        )

        # Send tracking id to the compacted topic (key+value = id string)
        producer.send(
            TOPIC_EPISODES_ID,
            key=eid_str.encode("utf-8") if eid_str else None,
            value=eid_str
        )

        print(f"Published new episode: {eid_str}")

    producer.flush()
    print(f"Finished: {len(new_episodes)} new episodes published.")

if __name__ == "__main__":
    discover_and_publish_new_episodes()