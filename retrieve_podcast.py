import requests
import hashlib
import time
import json
import os
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

#====== API PODCASTINDEX ==============

API_KEY = "4YCF385ZNXLEHADRUSFV"
API_SECRET = "YcjAfd9x7S$$qX^4u#n$rvLD$X6c^pT87ShWvF3V"

timestamp = str(int(time.time()))
auth_hash = hashlib.sha1((API_KEY + API_SECRET + timestamp).encode('utf-8')).hexdigest()

headers = {
    "X-Auth-Date": timestamp,
    "X-Auth-Key": API_KEY,
    "Authorization": auth_hash,
    "User-Agent": "PodcastIndex-Client"
}

#====== STEP 1: RETRIEVE TOP TRENDING PODCASTS =========

url = "https://api.podcastindex.org/api/1.0/podcasts/trending"
params = {"max": 50}

response = requests.get(url, headers=headers, params=params)

if response.status_code != 200:
    print("❌ Failed to retrieve trending podcasts")
    print(response.text)
    exit()

trending_podcasts = response.json().get("feeds", [])
print(f"Retrieved {len(trending_podcasts)} trending podcasts.")

#====== STEP 2: FILTER ENGLISH PODCASTS ==========

def is_english(feed):
    if feed.get("language", "").lower() == "en":
        return True
    try:
        text = f"{feed.get('title', '')} {feed.get('description', '')}"
        detected_lang = detect(text)
        return detected_lang == "en"
    except LangDetectException:
        return False

english_podcasts = [feed for feed in trending_podcasts if is_english(feed)]
print(f"Filtered down to {len(english_podcasts)} English podcasts.")

#====== STEP 3: SIMPLIFY + SAVE TO top_podcasts.json ==========

simplified_podcasts = [
    {
        "title": feed.get("title"),
        "url": feed.get("url"),
        "description": feed.get("description"),
        "id": feed.get("id"),
        "author": feed.get("author"),
    }
    for feed in english_podcasts
]

with open("top_podcasts.json", "w") as f:
    json.dump(simplified_podcasts, f, indent=4)

print(f"✅ Saved {len(simplified_podcasts)} podcasts to top_podcasts.json")

#====== STEP 4: FETCH EPISODES (LIMIT = 1) PER PODCAST ==========

all_episodes = []

for podcast in simplified_podcasts:
    feed_id = podcast.get("id")
    podcast_title = podcast.get("title")

    print(f"🔍 Fetching episode for: {podcast_title}")

    response = requests.get(
        "https://api.podcastindex.org/api/1.0/episodes/byfeedid",
        headers=headers,
        params={"id": feed_id, "max": 1}  # Adjust max for more episodes
    )

    if response.status_code == 200:
        episodes = response.json().get("items", [])
        for ep in episodes:
            audio_url = ep.get("enclosureUrl")
            if audio_url:
                all_episodes.append({
                    "podcast": podcast_title,
                    "episode_title": ep.get("title"),
                    "audio_url": audio_url,
                    "id": ep.get("id")
                })
    else:
        print(f" Failed to fetch episodes for: {podcast_title}")
        print(response.text)

#====== STEP 5: SAVE EPISODES TO episode_url.json ==========

with open("episode_url.json", "w") as f:
    json.dump(all_episodes, f, indent=4)

print(f"✅ Saved {len(all_episodes)} episodes to episode_url.json")
