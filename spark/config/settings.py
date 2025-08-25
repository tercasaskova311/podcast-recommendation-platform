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
DELTA_PATH_EVENTS = _os.getenv("DELTA_PATH_EVENTS", f"file://{_ROOT}/data/delta/user_events")
DELTA_PATH_DAILY  = _os.getenv("DELTA_PATH_DAILY",  f"file://{_ROOT}/data/delta/user_episode_daily")
CHK_PATH          = _os.getenv("CHK_PATH",          f"file://{_ROOT}/data/chk/user_events_stream")

# --- Weighting knobs (tweak if you like) ---
LIKE_W = 3.0
COMPLETE_W = 2.0
SKIP_W = -1.0
PAUSE_SEC_CAP = 600.0          # 600s -> +1
DECAY_HALFLIFE_DAYS = 14.0     # set to 0 to disable decay
