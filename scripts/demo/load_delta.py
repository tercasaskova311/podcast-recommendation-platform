#   PYTHONPATH="$PWD" python -m spark.scripts.demo.load_delta
from pyspark.sql import functions as F
from spark.util.common import get_spark
from spark.config.settings import (
    DELTA_PATH_EPISODES,
    DELTA_PATH_TRANSCRIPTS,
)

def read_sample_json(spark, path: str):
    if not path:
        raise ValueError
    df = (
        spark.read
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .json(path)
    )

    if df.columns == ["value"] and df.schema["value"].dataType.typeName() == "array":
        df = df.select(F.explode("value").alias("row")).select("row.*")
    if set(df.columns) == {"_corrupt_record"}:
        bad = spark.read.text(path).limit(5)
        bad.show(truncate=False)
        raise ValueError("Input is not valid JSON for Spark.")
    return df

def main():
    spark = get_spark("load-delta")

    if not DELTA_PATH_EPISODES:
        raise ValueError("DELTA_PATH_EPISODES is not set in config.settings")
    if not DELTA_PATH_TRANSCRIPTS:
        raise ValueError("DELTA_PATH_TRANSCRIPTS is not set in config.settings")

    df = read_sample_json(spark)

    df.show(5, truncate=False)
    df.printSchema()

    if "description" in df.columns:
        desc_col = F.col("description")
    elif "podcast_description" in df.columns:
        desc_col = F.col("podcast_description")
    else:
        desc_col = F.lit(None).alias("description")

    podcast_url = F.when(
        F.col("audio_url").isNotNull(),
        F.regexp_extract(F.col("audio_url"), r"^(https?://[^/]+)", 1)
    ).otherwise(F.lit(None))

    metadata = (
        df.select(
            F.col("podcast"),
            F.col("podcast_author"),
            F.col("episode_title"),
            desc_col.alias("podcast_description"),
            F.col("created_at"),
            F.col("audio_url"),
            F.col("episode_id").cast("long"),
        )
        .withColumn("analyzed", F.lit(False))
        .withColumn("failed",   F.lit(False))
        .withColumn("ts",       F.current_timestamp())
        .coalesce(1)
    )

    transcripts = (
        df.select(
            F.col("episode_id").cast("long"),
            (F.col("transcript") if "transcript" in df.columns else F.col("text")).alias("transcript"),
        )
        .coalesce(1)
    )

    (
        metadata.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(DELTA_PATH_EPISODES)
    )

    (
        transcripts.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(DELTA_PATH_TRANSCRIPTS)
    )
    spark.stop()

if __name__ == "__main__":
    main()
