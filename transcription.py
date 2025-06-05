import os
import io
import json
import time
import requests
import torch
from pydub import AudioSegment
from faster_whisper import WhisperModel
from multiprocessing import Pool, current_process

# --- CONFIG ---
MAX_WORKERS = 1  # Usually 1 for GPU; more if CPU only
MODEL_SIZE = "tiny.en" #lightweight model, faster then the model for all languages
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
COMPUTE_TYPE = "float16" if DEVICE == "cuda" else "int8"
os.environ["OMP_NUM_THREADS"] = "1" # reduce parallel thread conflicts

# Global model variable inside each process
model = None

def init_model():
    global model
    print(f"[{current_process().name}] Loading model on {DEVICE}")
    model = WhisperModel(MODEL_SIZE, device=DEVICE, compute_type=COMPUTE_TYPE)

def safe_filename(name):
    return name.replace(" ", "_").replace("/", "_").replace("\\", "_").replace(":", "-")

def stream_download(url): 
    with requests.get(url, stream=True) as r: #streaming instead of downloading data from url 
        r.raise_for_status()
        buffer = io.BytesIO() #create temporary - to hold the audio
        for chunk in r.iter_content(chunk_size=8192):
            buffer.write(chunk)
        buffer.seek(0)
        return buffer

def convert_to_wav(audio_buffer):
    audio = AudioSegment.from_file(audio_buffer) #holding audio files
    audio = audio.set_frame_rate(16000).set_channels(1) #can be changed - adjusting the audio quality for transcribtion
    return audio

def transcribe_episode(episode, chunk_length_ms=6 * 60 * 1000): #processing whole podcast in once is computatinaly very heavy, especially for longer audio
    global model
    if model is None:
        raise RuntimeError("Model not initialized in worker process")

    title = episode.get("episode_title", "unknown")
    url = episode.get("audio_url")
    
    filename = safe_filename(title)
    output_dir = "transcripts"
    json_path = os.path.join(output_dir, filename + ".json")
    
    if os.path.exists(json_path):
        print(f"[{title}] ⏩ Already processed. Skipping.")
        return
    
    start = time.time()
    print(f"[{title}] 🎙️ Starting")


    # Download audio stream
    dl_start = time.time()
    audio_stream = stream_download(url)
    print(f"[{title}] Download took {time.time() - dl_start:.2f}s")

    # Convert audio
    conv_start = time.time()
    wav_data = convert_to_wav(audio_stream)
    print(f"[{title}] Conversion took {time.time() - conv_start:.2f}s")

    # Chunk audio
    chunks = [wav_data[i:i+chunk_length_ms] for i in range(0, len(wav_data), chunk_length_ms)]

    all_segments = []

    # Transcribe chunks from memory (using WhisperModel API directly from bytes)
    for idx, chunk in enumerate(chunks):
        # Export chunk to bytes buffer instead of temp file
        buf = io.BytesIO()
        chunk.export(buf, format="wav")
        buf.seek(0)

        # Transcribe from in-memory wav bytes
        segments, _ = model.transcribe(buf, beam_size=1)
        all_segments.extend(segments)
        print(f"[{title}] ⏱️ Chunk {idx+1}/{len(chunks)} transcribed")

    transcript = " ".join(seg.text for seg in all_segments)

    # Save transcript
    output_dir = "transcripts"
    os.makedirs(output_dir, exist_ok=True)
    json_path = os.path.join(output_dir, safe_filename(title) + ".json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(transcript, f, ensure_ascii=False)

    print(f"[{title}] ✅ Done in {time.time() - start:.2f}s | Saved to {json_path}")

#=========== MAIN ====================
if __name__ == "__main__":
    start = time.time()

    with open("episode_url.json", "r") as f:
        episodes = json.load(f)

    os.makedirs("transcripts", exist_ok=True)

    # Use multiprocessing Pool with initializer to share model per worker
    from multiprocessing import Pool

    with Pool(processes=MAX_WORKERS, initializer=init_model) as pool:
        pool.map(transcribe_episode, episodes)

    print(f"\nDone in {time.time() - start:.2f} seconds")
