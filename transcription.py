import json
import os
import io
import time
import requests
import tempfile
import torch
from pydub import AudioSegment
from concurrent.futures import ThreadPoolExecutor, as_completed
from faster_whisper import WhisperModel  # 

# --- CONFIGURATION ---
MAX_WORKERS = 4
MODEL_SIZE = "base"  # Options: "tiny", "base", "small", "medium", "large"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
COMPUTE_TYPE = "float16" if DEVICE == "cuda" else "int8"  # for CPU use int8

# --- AUDIO UTILS ---

def safe_filename(name):
    return (
        name.replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "-")
    )

def convert_to_wav_bytes(audio_bytes):
    audio = AudioSegment.from_file(io.BytesIO(audio_bytes))
    audio = audio.set_frame_rate(16000).set_channels(1)
    wav_io = io.BytesIO()
    audio.export(wav_io, format="wav")
    return wav_io.getvalue()

# --- TRANSCRIPTION FUNCTION ---

def process_episode(episode):
    episode_title = episode.get("episode_title", "unknown_episode")
    audio_url = episode.get("audio_url")
    safe_title = safe_filename(episode_title)
    transcript_json_path = os.path.join("transcripts", f"{safe_title}.json")

    if os.path.exists(transcript_json_path):
        print(f"[SKIP] {episode_title} (already transcribed).")
        return

    try:
        print(f"[FETCHING] {episode_title}")
        response = requests.get(audio_url, stream=True)
        response.raise_for_status()
        audio_bytes = response.content

        print(f"[CONVERTING] {episode_title}")
        wav_data = convert_to_wav_bytes(audio_bytes)

        with tempfile.NamedTemporaryFile(suffix=".wav", delete=True) as tmp_wav:
            tmp_wav.write(wav_data)
            tmp_wav.flush()

            print(f"[TRANSCRIBING] {episode_title}")
            segments, _info = model.transcribe(tmp_wav.name)

            # Join all text segments
            full_transcript = " ".join([seg.text for seg in segments])

            with open(transcript_json_path, "w", encoding="utf-8") as f_out:
                json.dump({"transcript": full_transcript}, f_out, ensure_ascii=False, indent=2)

            print(f"[SAVED] {episode_title}")

    except Exception as e:
        print(f"[ERROR] {episode_title}: {e}")

# --- MAIN ---

if __name__ == "__main__":
    start_time = time.time()

    # Load episode list
    with open("top_episodes.json", "r") as f:
        audio_list = json.load(f)

    # Prepare output directory
    os.makedirs("transcripts", exist_ok=True)

    # Load faster-whisper model
    print(f"[MODEL] Loading Faster-Whisper model: {MODEL_SIZE} on {DEVICE}")
    model = WhisperModel(
        MODEL_SIZE,
        device=DEVICE,
        compute_type=COMPUTE_TYPE,
        local_files_only=False  # set True if you're offline with local model
    )

    # Use multithreading
    print(f"[THREADING] Using {MAX_WORKERS} threads")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_episode, ep) for ep in audio_list]
        for future in as_completed(futures):
            future.result()  # Will raise exceptions if any

    total_time = time.time() - start_time
    print(f"\n✅ Finished all in {total_time:.2f} seconds")
