import json
import io
from datetime import datetime
from pyspark.sql import Row
from faster_whisper import WhisperModel
from util.audio import stream_download, convert_to_wav

MODEL = WhisperModel("tiny.en", device="cuda", compute_type="int8")

def generate_transcript(wav_segment, chunk_ms=6 * 60 * 1000):
    try:
        chunks = [wav_segment[i:i+chunk_ms] for i in range(0, len(wav_segment), chunk_ms)]
        segments = []
        for chunk in chunks:
            buf = io.BytesIO()
            chunk.export(buf, format="wav")
            buf.seek(0)
            s, _ = MODEL.transcribe(buf, beam_size=1)
            segments.extend(s)
        return " ".join(seg.text for seg in segments)
    except Exception as e:
        print(f"Transcription error: {e}")
        return None

def process_batch(df):
    def process_row(row):
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

            return Row(
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
            return None

    return df.rdd.map(process_row).filter(lambda r: r is not None).toDF()
