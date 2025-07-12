import requests
import io
from pydub import AudioSegment

def stream_download(url):
    try:
        with requests.get(url, stream=True, timeout=20) as r:
            r.raise_for_status()
            buf = io.BytesIO()
            for chunk in r.iter_content(8192):
                buf.write(chunk)
            buf.seek(0)
            return buf
    except Exception as e:
        print(f"Audio download failed: {e}")
        return None

def convert_to_wav(audio_buffer):
    try:
        audio = AudioSegment.from_file(audio_buffer)
        return audio.set_frame_rate(16000).set_channels(1)
    except Exception as e:
        print(f"Audio conversion failed: {e}")
        return None
