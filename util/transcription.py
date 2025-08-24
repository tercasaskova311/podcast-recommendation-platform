# util/transcription.py
import os
import gc
import math
import tempfile
import subprocess
from typing import Dict, Any, List, Iterator

import requests
from faster_whisper import WhisperModel


TRANSCRIBE_BACKEND="vosk"

# -------- Tunables (env) --------
WHISPER_MODEL   = "tiny.en"
WHISPER_LOCAL_DIR="/models/faster-whisper-tiny.en"
WHISPER_DEVICE  = "cpu"     # "cpu" or "cuda"
WHISPER_COMPUTE = "float32"
WHISPER_BEAM    = 1
WHISPER_VAD     = True

MAX_AUDIO_DURATION_SEC = 3600    # skip > 60 min
DOWNLOAD_TIMEOUT_SEC   = 180
DOWNLOAD_CHUNK_BYTES   = 65536  # 64 KiB

# Use a dedicated download root to avoid reusing a bad cache
WHISPER_DOWNLOAD_ROOT = "/tmp/whisper_ct2_models"

FFMPEG_PATH = "ffmpeg"
FFPROBE_PATH = "ffprobe"

# Keep CPU libs from oversubscribing cores
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
os.environ.setdefault("CT2_USE_EXPERIMENTAL_PACKED_GEMM", "1")

_fw_MODEL = None  # lazy

# ----------------------------
# Vosk (CPU-friendly)
# ----------------------------
VOSK_MODEL_DIR = "/models/vosk-small-en"  # e.g., /models/vosk-small-en
_vosk_MODEL = None  # lazy


# ===== Common helpers =====
def _download_to_tmp(url: str) -> str:
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
    cmd = [FFPROBE_PATH, "-v", "error", "-show_entries", "format=duration",
           "-of", "default=noprint_wrappers=1:nokey=1", path]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        return float(out.decode().strip())
    except Exception:
        return math.inf


def _to_wav_16k_mono(src_path: str) -> str:
    wav_path = src_path + ".16k.wav"
    cmd = [FFMPEG_PATH, "-y", "-i", src_path, "-ac", "1", "-ar", "16000", "-f", "wav", wav_path]
    subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return wav_path


# ===== Whisper backend =====
def _assert_ct2_layout(path: str):
    need = {"model.bin", "config.json", "tokenizer.json", "vocabulary.txt"}
    have = set(os.listdir(path))
    missing = need - have
    if missing:
        raise RuntimeError(f"Model at {path} is not a CT2 checkpoint (missing: {sorted(missing)})")


def _fw_get_model():
    global _fw_MODEL
    if _fw_MODEL is not None:
        return _fw_MODEL

    from faster_whisper import WhisperModel  # lazy import
    if WHISPER_LOCAL_DIR:
        print(f"[transcription] loading LOCAL CT2 model: {WHISPER_LOCAL_DIR}")
        _assert_ct2_layout(WHISPER_LOCAL_DIR)
        _fw_MODEL = WhisperModel(WHISPER_LOCAL_DIR, device=WHISPER_DEVICE, compute_type=WHISPER_COMPUTE)
        return _fw_MODEL

    os.makedirs(WHISPER_DL_ROOT, exist_ok=True)
    print(f"[transcription] loading CT2 model by name: {WHISPER_MODEL} (root={WHISPER_DL_ROOT})")
    _fw_MODEL = WhisperModel(WHISPER_MODEL, device=WHISPER_DEVICE, compute_type=WHISPER_COMPUTE,
                             download_root=WHISPER_DL_ROOT)
    return _fw_MODEL


def _fw_transcribe(wav_path: str) -> Dict[str, Any]:
    model = _fw_get_model()
    segments, info = model.transcribe(
        wav_path,
        vad_filter=WHISPER_VAD,
        beam_size=WHISPER_BEAM,
    )
    text = " ".join(s.text for s in segments)
    return {
        "transcript": text,
        "language": info.language,
        "duration": float(info.duration),
        "failed": False,
        "error": None,
    }


# ===== Vosk backend =====
def _vosk_get_model():
    global _vosk_MODEL
    if _vosk_MODEL is not None:
        return _vosk_MODEL
    if not VOSK_MODEL_DIR or not os.path.isdir(VOSK_MODEL_DIR):
        raise RuntimeError("VOSK_MODEL_DIR is not set or not a directory.")
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

        if TRANSCRIBE_BACKEND == "vosk":
            tr = _vosk_transcribe(wav)
        else:
            tr = _fw_transcribe(wav)

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