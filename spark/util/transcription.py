import json
import io
from datetime import datetime
from pyspark.sql import Row
from faster_whisper import WhisperModel
from util.audio import stream_download, convert_to_wav

# Executor-side singleton
_MODEL = None

def _get_model():
    global _MODEL
    if _MODEL is None:
        # Create once per Python worker process on the executor
        _MODEL = WhisperModel("tiny.en", device="cpu", compute_type="int8")
    return _MODEL

def generate_transcript(wav_segment, chunk_ms=6 * 60 * 1000):
    try:
        chunks = [wav_segment[i:i+chunk_ms] for i in range(0, len(wav_segment), chunk_ms)]
        segments = []
        model = _get_model()
        for chunk in chunks:
            buf = io.BytesIO()
            chunk.export(buf, format="wav")
            buf.seek(0)
            s, _ = model.transcribe(buf, beam_size=1)  # no vad_filter => no torch needed
            segments.extend(s)
        return " ".join(seg.text for seg in segments)
    except Exception as e:
        print(f"Transcription error: {e}")
        return None

def _process_rows(iter_rows):
    for row in iter_rows:
        try:
            data = json.loads(row.json_str)
            episode_id = row.episode_id
            title = data.get("title", "")
            url = data.get("audio_url", "")
            publish_date = data.get("publish_date", None)

            audio_buf = stream_download(url)
            if not audio_buf:
                raise Exception("Download failed")
            wav = convert_to_wav(audio_buf)
            if not wav:
                raise Exception("Conversion failed")

            transcript = generate_transcript(wav)
            failed = transcript is None

            yield Row(
                episode_id=episode_id,
                title=title,
                audio_url=url,
                transcript=transcript,
                publish_date=publish_date,
                analyzed=False,
                failed=failed,
                ts=datetime.utcnow()
            )
        except Exception as e:
            print(f"Error processing episode {row.episode_id}: {e}")
            # skip row

def process_batch(df):
    return df.rdd.mapPartitions(_process_rows).toDF()
