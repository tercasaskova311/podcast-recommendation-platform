import os
import io
import json
import time
import signal
import torch
import requests
from pydub import AudioSegment
from kafka import KafkaConsumer, KafkaProducer
from multiprocessing import Pool, current_process
from faster_whisper import WhisperModel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import current_timestamp, to_date

# ====== CONFIG ======
MODEL_SIZE = "tiny.en"
DEVICE = "cpu"
COMPUTE_TYPE = "int8"
MAX_WORKERS = 2

KAFKA_URL = "kafka1:9092"
TOPIC_EPISODE_METADATA = os.getenv("TOPIC_EPISODE_METADATA", "episode-metadata")
TOPIC_TRANSCRIPTS = "transcripts-en"
DELTA_OUTPUT_PATH = "/data_lake/transcripts_en"

model = None
spark = None
producer = None

# ====== INIT ======
def init_worker():
    global model, spark, producer

    model = WhisperModel(MODEL_SIZE, device=DEVICE, compute_type=COMPUTE_TYPE)

    spark = SparkSession.builder \
        .appName("SaveTranscript") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

# ====== HELPERS ======
def safe_filename(name):
    return name.replace(" ", "_").replace("/", "_").replace("\\", "_").replace(":", "-")


def stream_audio(url):
    with requests.get(url, stream=True, timeout=10) as r:
        r.raise_for_status()
        buf = io.BytesIO()
        for chunk in r.iter_content(8192):
            buf.write(chunk)
        buf.seek(0)
        return buf


def convert_to_wav(audio_buffer):
    return AudioSegment.from_file(audio_buffer).set_frame_rate(16000).set_channels(1)


# ====== MAIN TRANSCRIBE FUNCTION ======
def transcribe_episode(episode, chunk_length_ms=6 * 60 * 1000):
    global model, spark, producer

    title = episode.get("episode_title", "unknown")
    audio_url = episode.get("audio_url")
    episode_id = episode.get("episode_id")

    if not audio_url or not episode_id:
        return

    try:
        audio_stream = stream_audio(audio_url)
        wav = convert_to_wav(audio_stream)
        chunks = [wav[i:i + chunk_length_ms] for i in range(0, len(wav), chunk_length_ms)]

        segments = []
        for i, chunk in enumerate(chunks):
            buf = io.BytesIO()
            chunk.export(buf, format="wav")
            buf.seek(0)
            s, _ = model.transcribe(buf, beam_size=1)
            segments.extend(s)

        transcript = " ".join(seg.text for seg in segments)

        # === Prepare DataFrame with timestamp and date
        from pyspark.sql import Row
        df = spark.createDataFrame([
            Row(episode_id=episode_id, transcript=transcript)
        ])
        df = df.withColumn("ingested_at", current_timestamp())
        df = df.withColumn("date", to_date("ingested_at"))

        # === Write to Delta, partitioned by date
        df.write.format("delta").mode("append").partitionBy("date").save(DELTA_OUTPUT_PATH)

<<<<<<< HEAD:spark/kafka_transcribe_consumer.py
        schema = StructType().add("episode_id", StringType()).add("transcript", StringType())
        df = spark.createDataFrame([(episode_id, transcript_text)], schema=schema)
        df.write.format("delta").mode("append").save(DELTA_OUTPUT_PATH)
        spark.stop()

        # Send transcript to Kafka

        print(f"[{title}]  Done")
=======
        # === Send transcript to Kafka
        producer.send(TOPIC_TRANSCRIPTS, {
            "episode_id": episode_id,
            "transcript": transcript
        })
>>>>>>> bad34718c82aefeafb1fe7e54599c0b8580dfa68:kafka_transcribe_consumer.py

    except Exception as e:
        print(f"[{title}] Error: {e}")


# ====== SHUTDOWN HANDLER ======
def shutdown(consumer, pool):
    print("shutdown")
    try:
        pool.terminate()
        pool.join()
        consumer.close()
    except Exception as e:
        print(f"Shutdown error: {e}")


# ====== MAIN LOOP ======
if __name__ == "__main__":
    consumer = KafkaConsumer(
        TOPIC_EPISODE_METADATA,
        bootstrap_servers=KAFKA_URL,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="transcription-group"
    )

    pool = Pool(processes=MAX_WORKERS, initializer=init_worker)

    try:
        print(" Transcription service running...")
        for message in consumer:
            episode = message.value
            pool.apply_async(transcribe_episode, args=(episode,))
    except KeyboardInterrupt:
        shutdown(consumer, pool)
    except Exception as e:
        print(f"Main loop error: {e}")
        shutdown(consumer, pool)
