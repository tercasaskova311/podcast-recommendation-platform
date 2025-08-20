#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# ---- Local Spark builder (Delta-enabled) ----
def get_spark(app_name: str = "load-delta-demo"):
    """
    Start a local SparkSession with Delta Lake.
    Adjust DELTA_PKG if your Spark version differs.
    - Spark 3.4/3.5 + Scala 2.12 → io.delta:delta-spark_2.12:3.2.0 is fine.
    """
    delta_pkg = os.getenv("DELTA_PKG", "io.delta:delta-spark_2.12:3.2.0")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", delta_pkg)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark

# ---- IO paths from env ----
KAFKA_URL = os.getenv("KAFKA_URL")  # unused here; kept for parity
TOPIC_EPISODES_ID = os.getenv("TOPIC_EPISODES_ID")
TOPIC_EPISODE_METADATA = os.getenv("TOPIC_EPISODE_METADATA")

SAMPLE_EPISODES_JSON_PATH = os.getenv("SAMPLE_EPISODES_JSON_PATH")
DELTA_PATH_EPISODES      = os.getenv("DELTA_PATH_EPISODES")
DELTA_PATH_TRANSCRIPTS   = os.getenv("DELTA_PATH_TRANSCRIPTS")

def read_sample_json(spark, path):
    if not path:
        raise ValueError("SAMPLE_EPISODES_JSON_PATH is not set.")
    df = (
        spark.read
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .json(path)
    )
    # If the file is a top-level array, Spark returns a single column "value"
    if df.columns == ["value"] and df.schema["value"].dataType.typeName() == "array":
        df = df.select(F.explode("value").alias("row")).select("row.*")
    if set(df.columns) == {"_corrupt_record"}:
        bad = spark.read.text(path).limit(5)
        print("⚠️ Parsed only _corrupt_record. First lines:")
        bad.show(truncate=False)
        raise ValueError("Input is not valid JSON for Spark.")
    return df

def main():
    spark = get_spark()
    df = read_sample_json(spark, SAMPLE_EPISODES_JSON_PATH)

    print("=== new_eps_df ===")
    df.show(5, truncate=False)
    df.printSchema()

    # pick description column safely
    if "description" in df.columns:
        desc_col = F.col("description")
    elif "podcast_description" in df.columns:
        desc_col = F.col("podcast_description")
    else:
        desc_col = F.lit(None).alias("description")

    # derive podcast_url if available
    podcast_url = F.when(
        F.col("audio_url").isNotNull(),
        F.regexp_extract(F.col("audio_url"), r"^(https?://[^/]+)", 1)
    ).otherwise(F.lit(None))

    metadata = (
        df.select(
            F.col("podcast_title"),
            F.col("podcast_author"),
            podcast_url.alias("podcast_url"),
            F.col("episode_title"),
            desc_col.alias("description"),
            F.col("audio_url"),
            F.col("episode_id").cast("long")
        )
        .withColumn("analyzed", F.lit(False))
        .withColumn("failed",   F.lit(False))
        .withColumn("ts",       F.current_timestamp())
        .coalesce(1)
    )

    transcripts = (
        df.select(
            F.col("episode_id").cast("long"),
            (F.col("transcript") if "transcript" in df.columns else F.col("text")).alias("transcript")
        ).coalesce(1)
    )

    # Sanity: output envs present
    for var in ("DELTA_PATH_EPISODES", "DELTA_PATH_TRANSCRIPTS"):
        if not globals()[var]:
            raise ValueError(f"{var} is not set.")

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

    print(f"Wrote {metadata.count()} rows to Delta\n"
          f"  metadata   → {DELTA_PATH_EPISODES}\n"
          f"  transcripts→ {DELTA_PATH_TRANSCRIPTS}")
    spark.stop()

if __name__ == "__main__":
    main()
