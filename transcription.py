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
MAX_WORKERS = 3
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
    safe_title = safe_filename(title)

    print(f"[{title}] Starting")

    # Download
    dl_start = time.time()
    audio_stream = stream_download(url)
    print(f"[{title}] Download took {time.time() - dl_start:.2f}s")

    # Convert to mono 16kHz WAV
    conv_start = time.time()
    wav_data = convert_to_wav(audio_stream)
    print(f"[{title}] Conversion took {time.time() - conv_start:.2f}s")

    # Write to temp file and transcribe
    trans_start = time.time()
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
        wav_data.export(tmp.name, format="wav")
        tmp.flush()
        tmp_path = tmp.name

    segments, _ = model.transcribe(tmp.name)

    os.remove(tmp_path)  # cleanup temp file
    print(f"[{title}] Transcription took {time.time() - trans_start:.2f}s")
    print(f"[{title}] Total time {time.time() - start:.2f}s")

    # Create simple transcript string
    transcript = " ".join(seg.text for seg in segments)

    output_dir = "transcripts"
    os.makedirs(output_dir, exist_ok=True)

    #save json
    json_path = os.path.join(output_dir, safe_filename(title) + ".json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(transcript, f, ensure_ascii=False)

    print(f"[{title}] ✅ Saved to {json_path}")


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
