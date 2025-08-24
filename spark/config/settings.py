# config/settings.py
import os

# Kafka
KAFKA_URL = os.getenv("KAFKA_URL", "localhost:9092")
TOPIC_USER_EVENTS_STREAMING = os.getenv("TOPIC_USER_EVENTS_STREAMING", "user-events-streaming")

# Optional other topics / Spark / Delta (keep if you use them elsewhere)
TOPIC_EPISODE_METADATA = os.getenv("TOPIC_EPISODE_METADATA")
SPARK_URL = os.getenv("SPARK_URL")
DELTA_PATH_EPISODES = os.getenv("DELTA_PATH_EPISODES")
DELTA_PATH_TRANSCRIPTS = os.getenv("DELTA_PATH_TRANSCRIPTS")
DELTA_PATH_VECTORS = os.getenv("DELTA_PATH_VECTORS")
DELTA_PATH_SIMILARITIES = os.getenv("DELTA_PATH_SIMILARITIES")

# Model + embedding
MODEL_NAME = os.getenv("MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
MAX_TOKENS = int(os.getenv("MAX_TOKENS", "512"))
OVERLAP = int(os.getenv("OVERLAP", "32"))
SAFETY_MARGIN = int(os.getenv("TOKEN_SAFETY_MARGIN", "8"))
TOP_K = int(os.getenv("TOP_K", "3"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "64"))
DEVICE = os.getenv("DEVICE", "cpu")

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "similarities")
# Where episode docs live (used by the producer to fetch episode_ids)
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "episodes")
EPISODE_ID_FIELD = os.getenv("EPISODE_ID_FIELD", "episode_id")

# Control flags
RECOMPUTE_ALL = os.getenv("RECOMPUTE_ALL", "false").lower() in ("1","true","yes","y")
WITHIN_BATCH_IF_EMPTY = os.getenv("WITHIN_BATCH_IF_EMPTY", "true").lower() in ("1","true","yes","y")
BATCH_DATE = os.getenv("BATCH_DATE")

# User event generation defaults
NUM_USERS = int(os.getenv("NUM_USERS", "300"))
MIN_EPS = int(os.getenv("MIN_EPS_PER_USER", "1"))
MAX_EPS = int(os.getenv("MAX_EPS_PER_USER", "5"))

# Optional Mongo filters for the producer
EPISODE_MATCH = os.getenv("EPISODE_MATCH", "")     # JSON string like {"language":"en"}
EPISODE_LIMIT = int(os.getenv("EPISODE_LIMIT", "0"))
EPISODE_SAMPLE_N = int(os.getenv("EPISODE_SAMPLE_N", "0"))
