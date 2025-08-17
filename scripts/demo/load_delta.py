import os
from pyspark.sql import functions as F
from util.common import get_spark

KAFKA_URL = os.getenv("KAFKA_URL")
TOPIC_EPISODES_ID = os.getenv("TOPIC_EPISODES_ID")
TOPIC_EPISODE_METADATA = os.getenv("TOPIC_EPISODE_METADATA")
SAMPLE_EPISODES_JSON_PATH = os.getenv("SAMPLE_EPISODES_JSON_PATH")
DELTA_PATH_EPISODES = os.getenv("DELTA_PATH_EPISODES")
DELTA_PATH_TRANSCRIPTS = os.getenv("DELTA_PATH_TRANSCRIPTS")

def main():
    spark = get_spark()

    #spark.conf.set("spark.sql.shuffle.partitions", "8")

    df = read_sample_json(spark, SAMPLE_EPISODES_JSON_PATH)

    print("=== new_eps_df ===")
    df.show(5, truncate=False)
    df.printSchema()

    metadata = (df.select(
        F.col("podcast_title"),
        F.col("podcast_author"),
        F.col("podcast_url"),
        F.col("episode_title"),
        F.col("description"),
        F.col("audio_url"),
        F.col("episode_id").cast("long")
    )
    .withColumn("analyzed", F.lit(False))
    .withColumn("failed", F.lit(False))
    .withColumn("ts", F.current_timestamp())
    .coalesce(1))

    transcripts = (df.select(
        F.col("episode_id").cast("long"),
        F.col("transcript")
    )
    .coalesce(1)
    )

    (metadata.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(DELTA_PATH_EPISODES))
    
    (transcripts.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(DELTA_PATH_TRANSCRIPTS))

    print(f"Wrote {metadata.count()} rows to Delta, metadata at {DELTA_PATH_EPISODES} and transcripts at {DELTA_PATH_TRANSCRIPTS}")
    spark.stop()

def read_sample_json(spark, path):
    df = (spark.read
            .option("multiLine", "true")   # <- handle pretty-printed / arrays
            .option("mode", "PERMISSIVE")  # default, but explicit
            .json(path))

    # If the file is a top-level array, Spark returns a single column named "value"
    # containing array<struct<...>>. Explode it to one row per element.
    if df.columns == ["value"] and df.schema["value"].dataType.typeName() == "array":
        df = df.select(F.explode("value").alias("row")).select("row.*")

    # Sanity check: if we still only have _corrupt_record, show a few bad lines to debug
    if set(df.columns) == {"_corrupt_record"}:
        bad = spark.read.text(path).limit(5)
        print("⚠️ File parsed as _corrupt_record only. First lines for inspection:")
        bad.show(truncate=False)
        raise ValueError("sample_episodes.json is not valid JSON for Spark. "
                         "If it’s a single big object, keep multiLine=true; "
                         "if it’s CSV or something else, use the right reader.")
    return df


if __name__ == "__main__":
    main()
