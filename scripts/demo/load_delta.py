#   Usage:
#     PYTHONPATH="$PWD" python -m spark.scripts.demo.load_delta \
#       --input /path/to/top_episodes.json
#
#   Or set SAMPLE_EPISODES_JSON_PATH in config/settings (or env) and run with no --input.

import os
import argparse
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException

from spark.util.common import get_spark
from spark.config.settings import (
    DELTA_PATH_EPISODES,
    DELTA_PATH_TRANSCRIPTS,
    # Optional: if your settings exposes this; otherwise we fall back to env var
    # SAMPLE_EPISODES_JSON_PATH,
)

# -------- helpers --------
def read_sample_json(spark: SparkSession, path: str):
    """
    Read a JSON file that may be:
      - a JSON array of objects, or
      - one JSON object per line (NDJSON), or
      - multi-line JSON objects.

    Returns a cleaned Spark DataFrame or raises a helpful error.
    """
    if not path:
        raise ValueError("Input path must be provided.")

    df = (
        spark.read
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .json(path)
    )

    # Case: file contains a single array under "value"
    if df.columns == ["value"] and df.schema["value"].dataType.typeName() == "array":
        df = df.select(F.explode("value").alias("row")).select("row.*")

    # If Spark considers everything corrupt, show a peek and fail
    if set(df.columns) == {"_corrupt_record"}:
        spark.read.text(path).limit(5).show(truncate=False)
        raise ValueError("Input is not valid JSON for Spark.")

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default=None,
                        help="Path to sample episodes JSON (array or NDJSON).")
    args = parser.parse_args()

    # Prefer CLI arg, else config var, else env var
    sample_path = (
        args.input
        or getattr(__import__("spark.config.settings", fromlist=[""]), "SAMPLE_EPISODES_JSON_PATH", None)
        or os.getenv("SAMPLE_EPISODES_JSON_PATH")
    )
    if not sample_path:
        raise ValueError("No input provided. Pass --input or set SAMPLE_EPISODES_JSON_PATH.")

    if not DELTA_PATH_EPISODES:
        raise ValueError("DELTA_PATH_EPISODES is not set in config.settings")
    if not DELTA_PATH_TRANSCRIPTS:
        raise ValueError("DELTA_PATH_TRANSCRIPTS is not set in config.settings")

    spark = get_spark("load-delta")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    df = read_sample_json(spark, sample_path)

    print("Sample rows from input JSON:")
    df.show(5, truncate=False)
    df.printSchema()

    # --- Normalize column names & types ---
    # Use 'description' if present; otherwise 'podcast_description'; else null
    if "description" in df.columns:
        desc_col = F.col("description")
    elif "podcast_description" in df.columns:
        desc_col = F.col("podcast_description")
    else:
        desc_col = F.lit(None)

    # Keep episode_id as STRING across the platform (important for joins with Mongo/Delta)
    episode_id_col = F.col("episode_id").cast("string")

    # If your JSON has a created_at string, we keep it; else write null
    created_at_col = F.col("created_at") if "created_at" in df.columns else F.lit(None)

    # Optional: derive a host from audio_url (handy for quick QA)
    podcast_host = F.when(
        F.col("audio_url").isNotNull(),
        F.regexp_extract(F.col("audio_url"), r"^(https?://[^/]+)", 1)
    ).otherwise(F.lit(None)).alias("audio_host")

    # ---------- Write EPISODES (metadata) ----------
    metadata = (
        df.select(
            F.col("podcast"),
            F.col("podcast_author"),
            F.col("episode_title"),
            desc_col.alias("podcast_description"),
            created_at_col.alias("created_at_raw"),
            F.col("audio_url"),
            episode_id_col.alias("episode_id"),
            podcast_host,
        )
        # light runtime columns for exploration
        .withColumn("analyzed", F.lit(False))   # harmless flag; not used by pipeline logic
        .withColumn("failed",   F.lit(False))
        .withColumn("ts",       F.current_timestamp())  # ingest timestamp (TIMESTAMP)
        .coalesce(1)  # demo-friendly single file; avoid in prod
    )

    # ---------- Write TRANSCRIPTS (content) ----------
    # Use 'transcript' if present, else fallback to 'text'
    text_col = F.col("transcript") if "transcript" in df.columns else F.col("text")
    transcripts = (
        df.select(
            episode_id_col.alias("episode_id"),
            text_col.alias("transcript")
        )
        .coalesce(1)
    )

    # Save to Delta (overwrite to reset demo data)
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

    print(f"Wrote episodes → {DELTA_PATH_EPISODES}")
    print(f"Wrote transcripts → {DELTA_PATH_TRANSCRIPTS}")

    spark.stop()


if __name__ == "__main__":
    main()
