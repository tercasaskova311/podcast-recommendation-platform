import os
import io
import json
import time
import tempfile
import requests
import torch
from pydub import AudioSegment
from faster_whisper import WhisperModel
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIG --------------------------------------------------------------------------------
MAX_WORKERS = 4
MODEL_SIZE = "tiny.en"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
COMPUTE_TYPE = "float16" if DEVICE == "cuda" else "int8"

# --- HELPERS -------------------------------------------------------------------------------

def safe_filename(name):
    return name.replace(" ", "_").replace("/", "_").replace("\\", "_").replace(":", "-")

def stream_download(url):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        buffer = io.BytesIO()
        for chunk in r.iter_content(chunk_size=8192):
            buffer.write(chunk)
        buffer.seek(0)
        return buffer

def convert_to_wav(audio_buffer):
    audio = AudioSegment.from_file(audio_buffer)
    audio = audio.set_frame_rate(16000).set_channels(1)
    return audio

# --- TRANSCRIPTION ------------------------------------------------------------------------

def transcribe_episode(episode):
    start = time.time()
    title = episode.get("episode_title", "unknown")
    url = episode.get("audio_url")
    print(f"\n[{title}] Starting")

    # Download
    dl_start = time.time()
    audio_stream = stream_download(url)
    print(f"[{title}] Download took {time.time() - dl_start:.2f}s")

    # Convert to AudioSegment
    conv_start = time.time()
    audio = convert_to_wav(audio_stream)
    print(f"[{title}] Conversion took {time.time() - conv_start:.2f}s")

    # --- STEP 1: Language detection from first 30s snippet
    snippet = audio[:30 * 1000]  # First 30 seconds
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=True) as temp_snippet:
        snippet.export(temp_snippet.name, format="wav")
        _, info = model.transcribe(temp_snippet.name, beam_size=1, language=None)
        detected_lang = info.language
        print(f"[{title}] Detected language: {detected_lang}")

        if detected_lang != "en":
            print(f"[{title}] Skipping non-English episode.\n")
            return

    # --- STEP 2: Full transcription
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=True) as tmp:
        audio.export(tmp.name, format="wav")
        segments, _ = model.transcribe(tmp.name, beam_size=5, language="en")
        transcript = " ".join(seg.text for seg in segments)

    print(f"[{title}] Total time: {time.time() - start:.2f}s")

    # Save to file
    filename = safe_filename(f"{title}.txt")
    with open(os.path.join("transcripts", filename), "w", encoding="utf-8") as f:
        f.write(transcript)

# --- MAIN ----------------------------------------------------------------------------------

if __name__ == "__main__":
    start = time.time()

    with open("top_episodes.json", "r") as f:
        episodes = json.load(f)

    os.makedirs("transcripts", exist_ok=True)

    print(f"[MODEL] Loading model '{MODEL_SIZE}' on {DEVICE}")
    model = WhisperModel(MODEL_SIZE, device=DEVICE, compute_type=COMPUTE_TYPE)

    print(f"[THREADING] Using {MAX_WORKERS} workers")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(transcribe_episode, ep) for ep in episodes]
        for future in as_completed(futures):
            future.result()

    print(f"\n Done in {time.time() - start:.2f} seconds")
