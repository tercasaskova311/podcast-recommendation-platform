import os

KAFKA_URL = os.getenv("KAFKA_URL")
TOPIC_EPISODE_METADATA = os.getenv("TOPIC_EPISODE_METADATA")
DELTA_PATH = os.getenv("DELTA_PATH")
SPARK_URL = os.getenv("SPARK_URL")