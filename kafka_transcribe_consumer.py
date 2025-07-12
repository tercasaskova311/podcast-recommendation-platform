import json
import os
import io
import time
import torch
import requests
from kafka import KafkaConsumer, KafkaProducer
from multiprocessing import Pool, current_process
from pydub import AudioSegment
from faster_whisper import WhisperModel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

# ====== CONFIG ======
MODEL_SIZE = "tiny.en"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
COMPUTE_TYPE = "int8"
MAX_WORKERS = 1
KAFKA_URL = "kafka1:9092"
TOPIC_RAW_PODCAST = os.getenv("TOPIC_RAW_PODCAST", "raw-podcast-metadata")
TOPIC_TRANSCRIPTS = "transcripts-en"
DELTA_OUTPUT_PATH = "/data_lake/transcripts_en"

os.environ["OMP_NUM_THREADS"] = "1"
model = None

# ====== SETUP WHISPER ======
def init_model():
    global model
    print(f"[{current_process().name}] Loading Whisper model...")
    model = WhisperModel(MODEL_SIZE, device=DEVICE, compute_type=COMPUTE_TYPE)

# ====== HELPERS ======
def safe_filename(name):
    return name.replace(" ", "_").replace("/", "_").replace("\\", "_").replace(":", "-")

def stream_download(url):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        buf = io.BytesIO()
        for chunk in r.iter_content(chunk_size=8192):
            buf.write(chunk)
        buf.seek(0)
        return buf

def convert_to_wav(audio_buffer):
    audio = AudioSegment.from_file(audio_buffer)
    return audio.set_frame_rate(16000).set_channels(1)

# ====== TRANSCRIPTION ======
def transcribe_episode(episode, chunk_length_ms=6 * 60 * 1000):
    global model
    if model is None:
        raise RuntimeError("Model not initialized")

    title = episode.get("episode_title", "unknown")
    url = episode.get("audio_url")
    episode_id = episode.get("episode_id")
    filename = safe_filename(title)
    local_output_path = os.path.join("transcripts", filename + ".json")

    if os.path.exists(local_output_path):
        print(f"[{title}] Skipped (already done)")
        return

    print(f"[{title}] Transcribing...")

    try:
        audio_stream = stream_download(url)
        wav = convert_to_wav(audio_stream)
        chunks = [wav[i:i+chunk_length_ms] for i in range(0, len(wav), chunk_length_ms)]

        segments = []
        for i, chunk in enumerate(chunks):
            buf = io.BytesIO()
            chunk.export(buf, format="wav")
            buf.seek(0)
            s, _ = model.transcribe(buf, beam_size=1)
            segments.extend(s)
            print(f"[{title}] Chunk {i+1}/{len(chunks)}")

        transcript_text = " ".join(seg.text for seg in segments)

        # Save locally (optional)
        os.makedirs("transcripts", exist_ok=True)
        with open(local_output_path, "w", encoding="utf-8") as f:
            json.dump(transcript_text, f, ensure_ascii=False)

        # Push to Delta Lake
        spark = SparkSession.builder \
            .appName("SaveTranscript") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

        schema = StructType().add("episode_id", StringType()).add("transcript", StringType())
        df = spark.createDataFrame([(episode_id, transcript_text)], schema=schema)
        df.write.format("delta").mode("append").save(DELTA_OUTPUT_PATH)
        spark.stop()

        # Send transcript to Kafka
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_URL,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        producer.send(TOPIC_TRANSCRIPTS, {
            "episode_id": episode_id,
            "transcript": transcript_text
        })
        producer.flush()
        producer.close()

        print(f"[{title}]  Done")

    except Exception as e:
        print(f"[{title}] Error: {e}")

# ====== MAIN LOOP ======
if __name__ == "__main__":
    print("🎧 Starting Kafka Transcription Consumer...")
    consumer = KafkaConsumer(
        TOPIC_RAW_PODCAST,
        bootstrap_servers=KAFKA_URL,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="transcription-group"
    )

    with Pool(processes=MAX_WORKERS, initializer=init_model) as pool:
        for message in consumer:
            episode = message.value
            pool.apply_async(transcribe_episode, (episode,))
