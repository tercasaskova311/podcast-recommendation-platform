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
from huggingface_hub import login
import os

# --- CONFIG --------------------------------------------------------------------------------


login(token=os.environ["HUGGINGFACE_TOKEN"])


MAX_WORKERS = 2
MODEL_SIZE = "tiny"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
COMPUTE_TYPE = "float16" if DEVICE == "cuda" else "int8"

# Save file names corectly => get url => convert to wav bytes----------------------------------
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

def convert_to_wav_bytes(audio_buffer):
    audio = AudioSegment.from_file(audio_buffer)
    audio = audio.set_frame_rate(16000).set_channels(1)
    wav_io = io.BytesIO()
    audio.export(wav_io, format="wav")
    return wav_io.getvalue()

# TRANSCRIBE -----------------------------------------------------------------------
import time

def transcribe_episode(episode):
    start = time.time()
    title = episode.get("episode_title", "unknown")
    url = episode.get("audio_url")

    print(f"[{title}] Starting")

    # Download
    dl_start = time.time()
    audio_stream = stream_download(url)
    print(f"[{title}] Download took {time.time() - dl_start:.2f}s")

    # Convert
    conv_start = time.time()
    wav_data = convert_to_wav_bytes(audio_stream)
    print(f"[{title}] Conversion took {time.time() - conv_start:.2f}s")

    # Write to temp and transcribe
    trans_start = time.time()
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=True) as tmp:
        tmp.write(wav_data)
        tmp.flush()
        segments, _ = model.transcribe(tmp.name)
        transcript = " ".join(seg.text for seg in segments)
    print(f"[{title}] Transcription took {time.time() - trans_start:.2f}s")

    print(f"[{title}] Total time {time.time() - start:.2f}s")


# --- MAIN ---

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
