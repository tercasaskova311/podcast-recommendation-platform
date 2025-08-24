# util/transcription.py
import os
import tempfile
import requests
from faster_whisper import WhisperModel
from pydub import AudioSegment

# ====== CONFIG ======
MODEL_SIZE = os.getenv("WHISPER_MODEL_SIZE", "tiny.en")
DEVICE = "cuda" if os.getenv("USE_CUDA", "0") == "1" else "cpu"
COMPUTE_TYPE = "int8"

# Load model once globally (not inside loop!)
_model = WhisperModel(MODEL_SIZE, device=DEVICE, compute_type=COMPUTE_TYPE)

def _download_audio(audio_url: str, target_path: str):
    resp = requests.get(audio_url, stream=True, timeout=60)
    resp.raise_for_status()
    with open(target_path, "wb") as f:
        for chunk in resp.iter_content(8192):
            f.write(chunk)

def _transcribe(audio_path: str):
    # Ensure it's wav (faster-whisper works better with wav/16k)
    wav_path = audio_path
    if not audio_path.endswith(".wav"):
        sound = AudioSegment.from_file(audio_path)
        wav_path = audio_path + ".wav"
        sound.export(wav_path, format="wav")

    segments, info = _model.transcribe(wav_path)
    transcript = " ".join([seg.text for seg in segments])
    return transcript, info

def process_batch(episodes):
    """
    Args:
        episodes: list of dicts with at least {"episode_id", "audio_url"}
    Yields:
        dict with transcript info for each episode
    """
    for ep in episodes:
        try:
            with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
                _download_audio(ep["audio_url"], tmp.name)
                transcript, info = _transcribe(tmp.name)
            yield {
                "episode_id": ep["episode_id"],
                "transcript": transcript,
                "language": info.language,
                "duration": info.duration,
                "failed": False,
            }
        except Exception as e:
            yield {
                "episode_id": ep.get("episode_id"),
                "transcript": None,
                "language": None,
                "duration": None,
                "failed": True,
                "error": str(e),
            }
