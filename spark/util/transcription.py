# util/transcription.py

import json
import io
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, TimestampType
)
from pyspark.sql import Row

# --- define the output schema used by _process_rows ---
OUTPUT_SCHEMA = StructType([
    StructField("episode_id",   LongType(),    True),
    StructField("title",        StringType(),  True),
    StructField("audio_url",    StringType(),  True),
    StructField("transcript",   StringType(),  True),
    StructField("analyzed",     BooleanType(), True),
    StructField("failed",       BooleanType(), True),
    StructField("ts",           TimestampType(), True),
])

# NOTE: DO NOT import faster_whisper or util.audio at module top-level.
# They will be imported lazily inside executor functions.

_MODEL = None

def _get_model():
    """Executor-side singleton for WhisperModel."""
    global _MODEL
    if _MODEL is None:
        # Lazy import here so the driver doesn't need faster_whisper installed
        from faster_whisper import WhisperModel
        _MODEL = WhisperModel("tiny.en", device="cpu", compute_type="int8")
    return _MODEL

def generate_transcript(wav_segment, chunk_ms=6 * 60 * 1000):
    try:
        import io
        model = _get_model()
        # Export chunks to in-memory WAV and transcribe
        chunks = [wav_segment[i:i+chunk_ms] for i in range(0, len(wav_segment), chunk_ms)]
        segments = []
        for chunk in chunks:
            buf = io.BytesIO()
            chunk.export(buf, format="wav")  # uses pydub on executor
            buf.seek(0)
            s, _ = model.transcribe(buf, beam_size=1)
            segments.extend(s)
        return " ".join(seg.text for seg in segments)
    except Exception as e:
        print(f"Transcription error: {e}")
        return None

def _process_rows(iter_rows):
    # lazy import executor-only stuff
    from util.audio import stream_download, convert_to_wav

    for row in iter_rows:
        try:
            data = json.loads(row.json_str)
            episode_id = row.episode_id
            url = data.get("audio_url", "")

            audio_buf = stream_download(url)
            if not audio_buf:
                print(f"[EXECUTOR] Download failed for episode {episode_id} url={url}")
                continue

            wav = convert_to_wav(audio_buf)
            if not wav:
                print(f"[EXECUTOR] Conversion failed for episode {episode_id}")
                continue

            transcript = generate_transcript(wav)
            failed = transcript is None

            yield Row(
                episode_id=episode_id,
                title=data.get("title", ""),
                audio_url=url,
                transcript=transcript,
                analyzed=False,
                failed=failed,
                ts=datetime.utcnow()
            )
        except Exception as e:
            print(f"[EXECUTOR] Error processing episode {getattr(row,'episode_id',None)}: {e}")
            # skip row


def process_batch(df):
    """
    Map each input row to a transcript row. Always return a DF with OUTPUT_SCHEMA,
    even when the RDD is empty.
    """
    required = {"episode_id", "json_str"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"process_batch missing required columns: {sorted(missing)}; got {df.columns}")
    
    # keep imports lazy so the driver doesn’t need ML deps
    rdd = (df.select("episode_id", "json_str")
             .rdd
             .mapPartitions(_process_rows))

    spark = df.sql_ctx.sparkSession
    if rdd.isEmpty():
        # Safe empty DF
        empty_rdd = spark.sparkContext.emptyRDD()
        return spark.createDataFrame(empty_rdd, schema=OUTPUT_SCHEMA)

    # Non-empty: create with explicit schema anyway (more robust)
    return spark.createDataFrame(rdd, schema=OUTPUT_SCHEMA)