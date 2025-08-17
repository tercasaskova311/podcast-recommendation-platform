from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from config.settings import KAFKA_URL, TOPIC_EPISODE_METADATA, DELTA_PATH_EPISODES, DELTA_PATH_TRANSCRIPTS
from util.delta import filter_new_episodes, upsert_episodes, upsert_transcripts
from util.transcription import process_batch
from util.kafka import read_episode_metadata
from util.common import get_spark

def run_pipeline():
    spark = get_spark()
 
    # 1) Ingest from Kafka to a *batch* dataframe of episode metadata
    kafka_df = read_episode_metadata(spark, KAFKA_URL, TOPIC_EPISODE_METADATA)

    cols = set(kafka_df.columns)
    if {"key", "value"}.issubset(cols):
        # returns raw Kafka cols
        raw = kafka_df.select(
            F.col("key").cast("string").alias("episode_id_str"),
            F.col("value").cast("string").alias("json_str")
        )
    elif {"episode_id", "json_str"}.issubset(cols):
        # your helper already did the cast/rename
        raw = kafka_df.select(
            F.col("episode_id").cast("string").alias("episode_id_str"),
            F.col("json_str").cast("string")
        )
    else:
        raise RuntimeError(f"Unexpected columns from read_episode_metadata: {kafka_df.columns}")

    episode_schema = StructType([
        StructField("podcast_title",  StringType(), True),
        StructField("podcast_author", StringType(), True),
        StructField("podcast_url",    StringType(), True),
        StructField("episode_title",  StringType(), True),
        StructField("description",    StringType(), True),
        StructField("audio_url",      StringType(), True),
        StructField("episode_id",     LongType(),   True),
    ])

    parsed = (raw
        .withColumn("data", F.from_json(F.col("json_str"), episode_schema))
        .select(
            F.coalesce(F.col("data.episode_id"),
                       F.col("episode_id_str").cast("long")).alias("episode_id"),
            F.col("json_str"),
            F.col("data.podcast_title"),
            F.col("data.podcast_author"),
            F.col("data.podcast_url"),
            F.col("data.episode_title"),
            F.col("data.description"),
            F.col("data.audio_url"),
        )
    )
    

    kafka_df = parsed

    print("=== kafka_df ===")
    kafka_df.printSchema()
    kafka_df.show(5, truncate=False)

    # 2) Keep only *new* episodes vs Delta episodes table
    new_eps_df = filter_new_episodes(spark, kafka_df, DELTA_PATH_EPISODES)

    # Early exit if nothing to do
    if new_eps_df.rdd.isEmpty():
        print("No new episodes to process.")
        spark.stop()
        return
    
    print("=== new_eps_df ===")
    new_eps_df.show(5, truncate=False)
    print("############ new_eps_df rows:", new_eps_df.count())

    # 3) Download transcripts for those new episodes
    transcripts_df = process_batch(new_eps_df).select("episode_id", "transcript")
    print("=== transcripts_df ===")
    # Save DataFrame as JSON (one file per partition)
    transcripts_df.write.mode("overwrite").json("/tmp/transcripts_output")


    #print("rows:", transcripts_df.count())
    #print("schema:"); transcripts_df.printSchema()

    # 4) Upsert BOTH tables
    #upsert_episodes(spark, new_eps_df, DELTA_PATH_EPISODES)
    #upsert_transcripts(spark, transcripts_df, DELTA_PATH_TRANSCRIPTS, insert_only=True)

    spark.stop()

    #GET ALL NEW EPISODES METADATA
    #CHECK IF THEY ARE ALREDY STORED IN DELTA
    #KEEP ONLY THE NEW ONES
    #DOWNLAOD THE AUDIO FILE
    #EXTRACT TRANSCRIPT (if some error occurs failed=true)
    #SAVE TRANSCRIPTS TO DELTA (analyzed = false)