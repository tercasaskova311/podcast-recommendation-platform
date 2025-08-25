import os

KAFKA_URL = os.getenv("KAFKA_URL")
TOPIC_EPISODE_METADATA = os.getenv("TOPIC_EPISODE_METADATA")
SPARK_URL = os.getenv("SPARK_URL")
DELTA_PATH_EPISODES = os.getenv("DELTA_PATH_EPISODES")
DELTA_PATH_TRANSCRIPTS = os.getenv("DELTA_PATH_TRANSCRIPTS")

DELTA_PATH_VECTORS = os.getenv("DELTA_PATH_VECTORS")
DELTA_PATH_SIMILARITIES = os.getenv("DELTA_PATH_SIMILARITIES")

# Model + embedding config
MODEL_NAME = os.getenv("MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
MAX_TOKENS = int(os.getenv("MAX_TOKENS", "512"))
OVERLAP = int(os.getenv("OVERLAP", "32"))
SAFETY_MARGIN = int(os.getenv("TOKEN_SAFETY_MARGIN", "8"))
TOP_K = int(os.getenv("TOP_K", "3"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "64"))
DEVICE = os.getenv("DEVICE", "cuda" if os.getenv("USE_CUDA", "1") == "1" else "cpu")

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "podcasts")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "similarities")

# Control flags
RECOMPUTE_ALL = os.getenv("RECOMPUTE_ALL", "false").lower() in ("1", "true", "yes", "y")
WITHIN_BATCH_IF_EMPTY = os.getenv("WITHIN_BATCH_IF_EMPTY", "true").lower() in ("1", "true", "yes", "y")
BATCH_DATE = os.getenv("BATCH_DATE")
