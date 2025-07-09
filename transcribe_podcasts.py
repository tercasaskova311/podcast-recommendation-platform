import requests
import hashlib
import time
import json
import os
import io
import torch
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException
from pydub import AudioSegment
from faster_whisper import WhisperModel
from multiprocessing import Pool, current_process

# --- CONFIG ---
MAX_WORKERS = 1
MODEL_SIZE = "tiny.en"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
COMPUTE_TYPE = "int8"
os.environ["OMP_NUM_THREADS"] = "1"

model = None

# ====== API PODCASTINDEX ==============
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

# ====== STEP 1–3: FETCH PODCAST EPISODES ==========
def is_english(feed):
    if feed.get("language", "").lower() == "en":
        return True
    try:
        text = f"{feed.get('title', '')} {feed.get('description', '')}"
        return detect(text) == "en"
    except LangDetectException:
        return False

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

        print(f"Fetching episode for: {podcast_title}")
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

    with open("episodes.json", "w") as f:
        json.dump(episodes, f, indent=4)

    return episodes

# ====== STEP 4: TRANSCRIPTION SETUP ==========
def init_model():
    global model
    print(f"[{current_process().name}] Loading Whisper model...")
    model = WhisperModel(MODEL_SIZE, device=DEVICE, compute_type=COMPUTE_TYPE)

def safe_filename(name):
    return name.replace(" ", "_").replace("/", "_").replace("\\", "_").replace(":", "-")

def stream_download(url):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        buf = io.BytesIO()
        for chunk in r.iter_content(chunk_size=8192):
            buf.write(chunk)
        buf.seek(0)
        return buf

def convert_to_wav(audio_buffer):
    audio = AudioSegment.from_file(audio_buffer)
    return audio.set_frame_rate(16000).set_channels(1)

def transcribe_episode(episode, chunk_length_ms=6*60*1000):
    global model
    if model is None:
        raise RuntimeError("Model not initialized")

    title = episode.get("episode_title", "unknown")
    url = episode.get("audio_url")
    filename = safe_filename(title)
    os.makedirs("transcripts", exist_ok=True)
    output_path = os.path.join("transcripts", filename + ".json")

    if os.path.exists(output_path):
        print(f"[{title}] ⏩ Skipped (already done)")
        return

    print(f"[{title}] 🎙️ Transcribing...")

    try:
        audio_stream = stream_download(url)
        wav = convert_to_wav(audio_stream)
        chunks = [wav[i:i+chunk_length_ms] for i in range(0, len(wav), chunk_length_ms)]

        segments = []
        for i, chunk in enumerate(chunks):
            buf = io.BytesIO()
            chunk.export(buf, format="wav")
            buf.seek(0)
            s, _ = model.transcribe(buf, beam_size=1)
            segments.extend(s)
            print(f"[{title}] ⏱️ Chunk {i+1}/{len(chunks)}")

        transcript = " ".join(seg.text for seg in segments)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(transcript, f, ensure_ascii=False)

        print(f"[{title}] Done")

    except Exception as e:
        print(f"[{title}]  Error: {e}")

# ========== MAIN ==========
if __name__ == "__main__":
    start = time.time()

    episodes = fetch_episodes()

    with Pool(processes=MAX_WORKERS, initializer=init_model) as pool:
        pool.map(transcribe_episode, episodes)

    print(f"\n All done in {time.time() - start:.2f}s")
