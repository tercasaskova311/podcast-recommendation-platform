import requests
import hashlib
import time
import json
import time
import os
import subprocess
from pathlib import Path
import sys
import tempfile


#====== API PODCASTINDEX ==============

API_KEY = "4YCF385ZNXLEHADRUSFV"
API_SECRET = "YcjAfd9x7S$$qX^4u#n$rvLD$X6c^pT87ShWvF3V"

#set up podcastindex api and retrive 50 treending podcast(random engagement/recent top50)

timestamp = str(int(time.time()))
auth_hash = hashlib.sha1((API_KEY + API_SECRET + timestamp).encode('utf-8')).hexdigest()

headers = {
    "X-Auth-Date": timestamp,
    "X-Auth-Key": API_KEY,
    "Authorization": auth_hash,
    "User-Agent": "PodcastIndex-Client"
}

#====== RETRIEVED TOP__ TRENDING PODCASTS =========
url = "https://api.podcastindex.org/api/1.0/podcasts/trending"
params = {"max": 50}  # Get up to 200 trending podcasts

response = requests.get(url, headers=headers, params=params)
trending_podcasts = response.json().get("feeds", [])

print(f"Retrieved {len(trending_podcasts)} trending podcasts.")



#===========SAVE TOP __ PODCASTS =============

url = "https://api.podcastindex.org/api/1.0/podcasts/trending"
response = requests.get(url, headers=headers, params=params)
trending_data = response.json()

#Extract relevant fields- only retrievable meta data from podcstindex
trending_data = response.json()
trending_podcasts = trending_data.get("feeds", [])

simplified_podcasts = [
    {
        "title": feed.get("title"),
        "url": feed.get("url"),
        "description": feed.get("description"),
        "id": feed.get("id"),
        "author": feed.get("author"),
    }
    for feed in trending_podcasts
]


#Save to JSON file
output_path = "top_podcasts.json"
with open(output_path, "w") as f:
    json.dump(simplified_podcasts, f, indent=4)

print(f"Saved {len(simplified_podcasts)} podcasts to {output_path}")

#===== SAVE EPISODES TO top_podcasts.json =============

with open("top_podcasts.json", "r") as f:
    podcasts = json.load(f)

all_episodes = []

for podcast in podcasts:
    feed_id = podcast.get("id")
    podcast_title = podcast.get("title")

    print(f"Fetching episodes for: {podcast_title}")

    response = requests.get(
        "https://api.podcastindex.org/api/1.0/episodes/byfeedid",
        headers=headers,
        params= {"id": feed_id, "max": 1} #more episodes from one podcast - change max
    )

    if response.status_code == 200:
        episodes = response.json().get("items", [])
        for ep in episodes:
            all_episodes.append({
                "podcast": podcast_title,
                "episode_title": ep.get("title"),
                "audio_url": ep.get("enclosureUrl"),
                "id": ep.get("id")
            })
    

with open("top_episodes.json", "w") as f:
    json.dump(all_episodes, f, indent=4)

print(f"✅ Saved {len(all_episodes)} episodes to top_episodes.json")
