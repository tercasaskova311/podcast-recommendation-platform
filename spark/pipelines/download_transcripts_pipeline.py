from pyspark.sql import SparkSession
from config.settings import KAFKA_URL, TOPIC_EPISODE_METADATA, DELTA_PATH
from util.delta import get_unprocessed_episodes, upsert_transcripts
from util.transcription import process_batch
from util.kafka import read_episode_metadata
from util.common import get_spark

def run_pipeline():
    spark = get_spark()

    kafka_df = read_episode_metadata(spark, KAFKA_URL, TOPIC_EPISODE_METADATA)
    print("=== kafka_df ===")
    kafka_df.printSchema()
    kafka_df.show(5, truncate=False)

    filtered_df = get_unprocessed_episodes(spark, kafka_df, DELTA_PATH)
    print("=== filtered_df ===")
    filtered_df.printSchema()
    filtered_df.show(5, truncate=False)
    
    # transcript_df = process_batch(filtered_df)

    # if not transcript_df.rdd.isEmpty():
    #     upsert_transcripts(spark, transcript_df, DELTA_PATH)

    spark.stop()

    #GET ALL NEW EPISODES METADATA
    #CHECK IF THEY ARE ALREDY STORED IN DELTA
    #KEEP ONLY THE NEW ONES
    #DOWNLAOD THE AUDIO FILE
    #EXTRACT TRANSCRIPT (if some error occurs failed=true)
    #SAVE TRANSCRIPTS TO DELTA (analyzed = false)