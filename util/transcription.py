# util/transcription.py
import os
import gc
import math
import tempfile
import subprocess
from typing import Dict, Any, List, Iterator

import requests
from faster_whisper import WhisperModel

# -------- Tunables (env) --------
WHISPER_MODEL   = os.getenv("WHISPER_MODEL", "tiny.en")
WHISPER_DEVICE  = os.getenv("WHISPER_DEVICE", "cpu")     # "cpu" or "cuda"
WHISPER_COMPUTE = os.getenv("WHISPER_COMPUTE", "int8")   # "int8", "int8_float16", "float16"
WHISPER_BEAM    = int(os.getenv("WHISPER_BEAM_SIZE", "1"))
WHISPER_VAD     = os.getenv("WHISPER_VAD_FILTER", "1") == "1"

MAX_AUDIO_DURATION_SEC = int(os.getenv("MAX_AUDIO_DURATION_SEC", "3600"))    # skip > 60 min
DOWNLOAD_TIMEOUT_SEC   = int(os.getenv("DOWNLOAD_TIMEOUT_SEC", "180"))
DOWNLOAD_CHUNK_BYTES   = int(os.getenv("DOWNLOAD_CHUNK_BYTES", str(1 << 16)))  # 64 KiB

FFMPEG_PATH  = os.getenv("FFMPEG_PATH", "ffmpeg")
FFPROBE_PATH = os.getenv("FFPROBE_PATH", "ffprobe")

# Keep CPU libs from oversubscribing cores
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
os.environ.setdefault("CT2_USE_EXPERIMENTAL_PACKED_GEMM", "1")

_MODEL = None  # lazy-loaded


# -------- Internal helpers --------
def _get_model() -> WhisperModel:
    """Lazy-load faster-whisper model; if cache is corrupted, clear once and retry."""
    global _MODEL
    if _MODEL is not None:
        return _MODEL
    try:
        _MODEL = WhisperModel(WHISPER_MODEL, device=WHISPER_DEVICE, compute_type=WHISPER_COMPUTE)
        return _MODEL
    except Exception:
        # Clear possible broken caches and retry once
        for p in (os.path.expanduser("~/.cache/huggingface/hub"),
                  os.path.expanduser("~/.cache/ctranslate2")):
            if os.path.isdir(p):
                try:
                    for name in os.listdir(p):
                        if "whisper" in name.lower() or "faster-whisper" in name.lower():
                            import shutil
                            shutil.rmtree(os.path.join(p, name), ignore_errors=True)
                except Exception:
                    pass
        gc.collect()
        _MODEL = WhisperModel(WHISPER_MODEL, device=WHISPER_DEVICE, compute_type=WHISPER_COMPUTE)
        return _MODEL


def _download_to_tmp(url: str) -> str:
    """Stream audio to a temp file (no large buffers in RAM)."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".audio") as tmp:
        tmp_path = tmp.name
    with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_SEC) as r:
        r.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(DOWNLOAD_CHUNK_BYTES):
                if chunk:
                    f.write(chunk)
    return tmp_path


def _probe_duration_seconds(path: str) -> float:
    """Ask ffprobe for precise duration without loading audio into Python."""
    cmd = [FFPROBE_PATH, "-v", "error", "-show_entries", "format=duration",
           "-of", "default=noprint_wrappers=1:nokey=1", path]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        return float(out.decode().strip())
    except Exception:
        # If probe fails, assume it's too large to be safe
        return math.inf


def _to_wav_16k_mono(src_path: str) -> str:
    """Transcode to 16 kHz mono WAV (small, Whisper-friendly)."""
    wav_path = src_path + ".16k.wav"
    cmd = [FFMPEG_PATH, "-y", "-i", src_path, "-ac", "1", "-ar", "16000", "-f", "wav", wav_path]
    subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return wav_path


# -------- Public API --------
def process_one(episode: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a single episode dict:
      { "episode_id": "...", "audio_url": "..." , ... }
    Returns:
      { episode_id, transcript, language, duration, failed, error }
    """
    eid = str(episode.get("episode_id")) if episode.get("episode_id") is not None else None
    url = episode.get("audio_url")

    if not eid or not url:
        return {
            "episode_id": eid, "transcript": None, "language": None,
            "duration": None, "failed": True, "error": "missing id/url"
        }

    src = None
    wav = None
    try:
        # 1) Download to disk (streamed)
        src = _download_to_tmp(url)

        # 2) Guard on duration
        dur = _probe_duration_seconds(src)
        if dur > MAX_AUDIO_DURATION_SEC:
            raise RuntimeError(f"audio too long: {dur:.1f}s > {MAX_AUDIO_DURATION_SEC}s")

        # 3) Transcode to compact WAV
        wav = _to_wav_16k_mono(src)

        # 4) Transcribe
        model = _get_model()
        segments, info = model.transcribe(wav, vad_filter=WHISPER_VAD, beam_size=WHISPER_BEAM)
        text = " ".join(s.text for s in segments)

        return {
            "episode_id": eid,
            "transcript": text,
            "language": info.language,
            "duration": float(info.duration),
            "failed": False,
            "error": None,
        }

    except Exception as e:
        return {
            "episode_id": eid, "transcript": None, "language": None,
            "duration": None, "failed": True, "error": str(e)
        }
    finally:
        # Clean up temp files promptly
        for p in (src, wav):
            if p:
                try:
                    os.remove(p)
                except Exception:
                    pass
        gc.collect()


def process_batch(episodes: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    """
    Generator that processes episodes **sequentially** (safe on memory/CPU).
    For each episode in `episodes`, yields the same shape as `process_one`.
    """
    for ep in episodes:
        yield process_one(ep)
