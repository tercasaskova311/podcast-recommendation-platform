# config/settings.py
import os

# Kafka
KAFKA_URL = os.getenv("KAFKA_URL", "localhost:9092")
TOPIC_USER_EVENTS_STREAMING = os.getenv("TOPIC_USER_EVENTS_STREAMING", "user-events-streaming")

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "podcasts")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "similarities")   # <- use similarities
EPISODE_ID_FIELD = os.getenv("EPISODE_ID_FIELD", "new_episode_id") # <- not "episode_id"



# User event generation defaults
NUM_USERS = int(os.getenv("NUM_USERS", "300"))
MIN_EPS = int(os.getenv("MIN_EPS_PER_USER", "1"))
MAX_EPS = int(os.getenv("MAX_EPS_PER_USER", "5"))

# Optional Mongo filters for the producer
EPISODE_LIMIT = int(os.getenv("EPISODE_LIMIT", "0"))
EPISODE_SAMPLE_N = int(os.getenv("EPISODE_SAMPLE_N", "0"))

# --- Kafka ---
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "127.0.0.1:9092")

# --- Delta locations (use file:// for local dev) ---
import os as _os
_ROOT = _os.getenv("DATA_ROOT", _os.getcwd())
DELTA_PATH_DAILY  = _os.getenv("DELTA_PATH_DAILY",  f"file://{_ROOT}/data/delta/user_episode_daily")
USER_EVENT_STREAM          = _os.getenv("CHK_PATH",          f"file://{_ROOT}/data/chk/user_events_stream")

# --- Weighting knobs (tweak if you like) ---
LIKE_W = 3.0
COMPLETE_W = 2.0
SKIP_W = -1.0
PAUSE_SEC_CAP = 600.0          # 600s -> +1
DECAY_HALFLIFE_DAYS = 14.0     # set to 0 to disable decay

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
MIN_ENGAGEMENT = float(_os.getenv("MIN_ENGAGEMENT", "1e-6"))

