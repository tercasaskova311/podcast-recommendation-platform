import os

# --- Kafka ---
KAFKA_URL = os.getenv("KAFKA_URL", "localhost:9092")
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "127.0.0.1:9092")
TOPIC_EPISODE_METADATA = os.getenv("TOPIC_EPISODE_METADATA")
TOPIC_USER_EVENTS_STREAMING = os.getenv("TOPIC_USER_EVENTS_STREAMING", "user-events-streaming")

# --- Spark / Delta paths ---
SPARK_URL = os.getenv("SPARK_URL")

DELTA_PATH_EPISODES     = os.getenv("DELTA_PATH_EPISODES")
DELTA_PATH_TRANSCRIPTS  = os.getenv("DELTA_PATH_TRANSCRIPTS")
DELTA_PATH_VECTORS      = os.getenv("DELTA_PATH_VECTORS")
DELTA_PATH_SIMILARITIES = os.getenv("DELTA_PATH_SIMILARITIES")

# Local dev / root paths
import os as _os
_ROOT = _os.getenv("DATA_ROOT", _os.getcwd())
DELTA_PATH_DAILY  = _os.getenv("DELTA_PATH_DAILY", f"file://{_ROOT}/data/delta/user_episode_daily")
USER_EVENT_STREAM = _os.getenv("CHK_PATH", f"file://{_ROOT}/data/chk/user_events_stream")

# --- Model + embedding config ---
MODEL_NAME    = os.getenv("MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
MAX_TOKENS    = int(os.getenv("MAX_TOKENS", "512"))
OVERLAP       = int(os.getenv("OVERLAP", "32"))
SAFETY_MARGIN = int(os.getenv("TOKEN_SAFETY_MARGIN", "8"))
TOP_K         = int(os.getenv("TOP_K", "3"))
BATCH_SIZE    = int(os.getenv("BATCH_SIZE", "64"))
DEVICE        = os.getenv("DEVICE", "cuda" if os.getenv("USE_CUDA", "1") == "1" else "cpu")

# --- MongoDB ---
MONGO_URI        = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB         = os.getenv("MONGO_DB", "podcasts")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "similarities")   # <- default to similarities
EPISODE_ID_FIELD = os.getenv("EPISODE_ID_FIELD", "new_episode_id") # <- not "episode_id"

# --- Control flags ---
RECOMPUTE_ALL        = os.getenv("RECOMPUTE_ALL", "false").lower() in ("1", "true", "yes", "y")
WITHIN_BATCH_IF_EMPTY= os.getenv("WITHIN_BATCH_IF_EMPTY", "true").lower() in ("1", "true", "yes", "y")
BATCH_DATE           = os.getenv("BATCH_DATE")

# --- User event generation defaults ---
NUM_USERS  = int(os.getenv("NUM_USERS", "300"))
MIN_EPS    = int(os.getenv("MIN_EPS_PER_USER", "1"))
MAX_EPS    = int(os.getenv("MAX_EPS_PER_USER", "5"))
EPISODE_LIMIT   = int(os.getenv("EPISODE_LIMIT", "0"))
EPISODE_SAMPLE_N= int(os.getenv("EPISODE_SAMPLE_N", "0"))

# --- Weighting knobs ---
LIKE_W  = 3.0
COMPLETE_W = 2.0
SKIP_W  = -1.0
PAUSE_SEC_CAP       = 600.0
DECAY_HALFLIFE_DAYS = 14.0

# --- ALS / Recommendations ---
ALS_MODEL_PATH    = _os.getenv("ALS_MODEL_PATH",    f"file://{_ROOT}/models/als_model")
ALS_INDEXERS_PATH = _os.getenv("ALS_INDEXERS_PATH", f"file://{_ROOT}/models/als_model_indexers")
ALS_DELTA_PATH    = _os.getenv("ALS_DELTA_PATH",    f"file://{_ROOT}/data/delta/als_predictions")
ALS_ITEMITEM_PATH = _os.getenv("ALS_ITEMITEM_PATH", f"file://{_ROOT}/data/delta/als_item_item")

TOP_N         = int(_os.getenv("ALS_TOP_N", "10"))
ALS_RANK      = int(_os.getenv("ALS_RANK", "64"))
ALS_REG       = float(_os.getenv("ALS_REG", "0.08"))
ALS_MAX_ITER  = int(_os.getenv("ALS_MAX_ITER", "15"))
ALS_ALPHA     = float(_os.getenv("ALS_ALPHA", "40.0"))
MIN_ENGAGEMENT= float(_os.getenv("MIN_ENGAGEMENT", "1e-6"))
