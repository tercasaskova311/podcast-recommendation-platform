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
MAX_WORKERS = 2 #episodes transcribed at once..
MODEL_SIZE = "tiny.en" #lightweight model, faster then the model for all languages
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
COMPUTE_TYPE = "float16" if DEVICE == "cuda" else "int8"

# --- HELPERS -------------------------------------------------------------------------------

def safe_filename(name):
    return name.replace(" ", "_").replace("/", "_").replace("\\", "_").replace(":", "-")

def stream_download(url): 
    with requests.get(url, stream=True) as r: #streaming instead of downloading data from url
        r.raise_for_status()
        buffer = io.BytesIO() #create temporary - to hold the audio
        
        for chunk in r.iter_content(chunk_size=8192): #loop through chunks - chunks are little faster to process
            buffer.write(chunk)
        
        buffer.seek(0)
        return buffer

def convert_to_wav(audio_buffer): 
    audio = AudioSegment.from_file(audio_buffer) #holding audio files
    audio = audio.set_frame_rate(16000).set_channels(1) #resample 16k Hz - can be change for better quality
    return audio


# --- TRANSCRIPTION ------------------------------------------------------------------------
def transcribe_episode(episode, chunk_length_ms=5 * 60 * 1000): #better to split longer audio in chunks, because otherwise whisper process it in once and sometimes it crush the memory, this should be solid
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

    # Chunk audio
    chunks = [wav_data[i:i+chunk_length_ms] for i in range(0, len(wav_data), chunk_length_ms)]

    all_segments = []

    for idx, chunk in enumerate(chunks):
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
            chunk.export(tmp.name, format="wav")
            tmp.flush()
            chunk_path = tmp.name

        try:
            segments, _ = model.transcribe(chunk_path)
            all_segments.extend(segments)
        finally:
            os.remove(chunk_path)

        print(f"[{title}] ⏱️ Chunk {idx+1}/{len(chunks)} transcribed")

    # Create full transcript
    transcript = " ".join(seg.text for seg in all_segments)

    # Save
    output_dir = "transcripts"
    os.makedirs(output_dir, exist_ok=True)
    json_path = os.path.join(output_dir, safe_filename(title) + ".json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(transcript, f, ensure_ascii=False)

    print(f"[{title}] ✅ Done in {time.time() - start:.2f}s | Saved to {json_path}")



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
