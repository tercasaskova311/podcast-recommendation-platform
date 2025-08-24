# util/transcription.py
import os
import gc
import math
import tempfile
import subprocess
from typing import Dict, Any, List, Iterator

import requests

# -------- Tunables (env) --------
MAX_AUDIO_DURATION_SEC = 5400  # skip > 90 min
DOWNLOAD_TIMEOUT_SEC   = 180
DOWNLOAD_CHUNK_BYTES   = (64 * 1024)  # 64 KiB

FFMPEG_PATH  = "ffmpeg"
FFPROBE_PATH = "ffprobe"

# Keep CPU libs from oversubscribing cores
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")

# ----------------------------
# Vosk (CPU-friendly)
# ----------------------------
VOSK_MODEL_DIR="/models/vosk-small-en"
_vosk_MODEL = None  # lazy singleton


# ===== Common helpers =====
def _download_to_tmp(url: str) -> str:
    # Keep original extension unknown; we’ll transcode anyway
    with tempfile.NamedTemporaryFile(delete=False, suffix=".src") as tmp:
        tmp_path = tmp.name
    with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_SEC) as r:
        r.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(DOWNLOAD_CHUNK_BYTES):
                if chunk:
                    f.write(chunk)
    return tmp_path


def _probe_duration_seconds(path: str) -> float:
    cmd = [FFPROBE_PATH, "-v", "error", "-show_entries", "format=duration",
           "-of", "default=noprint_wrappers=1:nokey=1", path]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        return float(out.decode().strip())
    except Exception:
        # If probing fails, let it pass through but mark as too long to be safe
        return math.inf


def _to_wav_16k_mono(src_path: str) -> str:
    wav_path = src_path + ".16k.wav"
    cmd = [FFMPEG_PATH, "-y", "-i", src_path, "-ac", "1", "-ar", "16000", "-f", "wav", wav_path]
    subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return wav_path


# ===== Vosk backend =====
def _vosk_get_model():
    global _vosk_MODEL
    if _vosk_MODEL is not None:
        return _vosk_MODEL
    if not VOSK_MODEL_DIR or not os.path.isdir(VOSK_MODEL_DIR):
        raise RuntimeError(f"VOSK_MODEL_DIR is not set or not a directory: {VOSK_MODEL_DIR}")
    from vosk import Model as VoskModel  # lazy import
    print(f"[transcription] loading Vosk model: {VOSK_MODEL_DIR}")
    _vosk_MODEL = VoskModel(VOSK_MODEL_DIR)
    return _vosk_MODEL


def _vosk_transcribe(wav_path: str) -> Dict[str, Any]:
    from vosk import KaldiRecognizer
    import json as _json
    import wave

    model = _vosk_get_model()
    with wave.open(wav_path, "rb") as wf:
        if wf.getnchannels() != 1 or wf.getframerate() != 16000:
            raise RuntimeError("WAV not 16k mono as expected.")
        rec = KaldiRecognizer(model, 16000)
        rec.SetWords(True)
        texts: List[str] = []
        # Read ~0.25s at 16 kHz, 16-bit mono -> 8000 bytes ≈ 4000 frames
        while True:
            data = wf.readframes(4000)
            if len(data) == 0:
                break
            if rec.AcceptWaveform(data):
                j = _json.loads(rec.Result())
                if "text" in j and j["text"]:
                    texts.append(j["text"])
        j = _json.loads(rec.FinalResult())
        if "text" in j and j["text"]:
            texts.append(j["text"])
    text = " ".join(t for t in texts if t)
    return {
        "transcript": text if text else None,
        "language": "en",
        "duration": None,
        "failed": False if text else True,
        "error": None if text else "empty transcription",
    }


# ===== Public API =====
def process_one(episode: Dict[str, Any]) -> Dict[str, Any]:
    eid = str(episode.get("episode_id")) if episode.get("episode_id") is not None else None
    url = episode.get("audio_url")

    if not eid or not url:
        return {"episode_id": eid, "transcript": None, "language": None,
                "duration": None, "failed": True, "error": "missing id/url"}

    src = None
    wav = None
    try:
        src = _download_to_tmp(url)
        dur = _probe_duration_seconds(src)
        if dur > MAX_AUDIO_DURATION_SEC:
            raise RuntimeError(f"audio too long: {dur:.1f}s > {MAX_AUDIO_DURATION_SEC}s")

        wav = _to_wav_16k_mono(src)
        tr = _vosk_transcribe(wav)

        return {
            "episode_id": eid,
            "transcript": tr.get("transcript"),
            "language": tr.get("language"),
            "duration": tr.get("duration"),
            "failed": bool(tr.get("failed", False)),
            "error": tr.get("error"),
        }

    except Exception as e:
        return {"episode_id": eid, "transcript": None, "language": None,
                "duration": None, "failed": True, "error": str(e)}
    finally:
        for p in (src, wav):
            if p:
                try:
                    os.remove(p)
                except Exception:
                    pass
        gc.collect()


def process_batch(episodes: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    for ep in episodes:
        yield process_one(ep)