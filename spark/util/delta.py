from delta.tables import DeltaTable
from pyspark.sql import functions as F


EPISODE_PK = "episode_id"

def _ensure_table(spark, path, df_sample):
    """Create empty Delta table w/ df_sample schema if it doesn't exist."""
    if DeltaTable.isDeltaTable(spark, path):
        return
    (df_sample.limit(0)
        .write.format("delta")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .save(path))
    # Table props are optional; ignore failures if FS doesn't support SQL DDL
    try:
        spark.sql(f"""
          ALTER TABLE delta.`{path}` SET TBLPROPERTIES(
            delta.autoOptimize.optimizeWrite=true,
            delta.autoOptimize.autoCompact=true
          )
        """)
    except Exception:
        pass

def filter_new_episodes(spark, kafka_df, episodes_delta_path):
    """Return only rows whose episode_id doesn't exist in episodes Delta."""
    if DeltaTable.isDeltaTable(spark, episodes_delta_path):
        existing_ids = (spark.read.format("delta")
                        .load(episodes_delta_path)
                        .select(EPISODE_PK).distinct())
        return kafka_df.join(existing_ids, on=EPISODE_PK, how="left_anti")
    return kafka_df

def upsert_episodes(spark, episodes_df, episodes_delta_path):
    """
    Upsert metadata keyed by episode_id. Expects columns:
      - episode_id (int/long)
      - podcast_title, podcast_author, podcast_url
      - episode_title, description, audio_url
    """
    now = F.current_timestamp()
    if "created_at" not in episodes_df.columns:
        episodes_df = episodes_df.withColumn("created_at", now)
    episodes_df = episodes_df.withColumn("updated_at", now)

    _ensure_table(spark, episodes_delta_path, episodes_df)

    tgt = DeltaTable.forPath(spark, episodes_delta_path)
    src_cols = [c for c in episodes_df.columns if c != EPISODE_PK]
    set_clause = {f"target.{c}": F.col(f"source.{c}") for c in src_cols}

    (tgt.alias("target")
        .merge(episodes_df.alias("source"), f"target.{EPISODE_PK} = source.{EPISODE_PK}")
        .whenMatchedUpdate(set=set_clause)     # update metadata if it changes
        .whenNotMatchedInsertAll()
        .execute())

def upsert_transcripts(spark, transcripts_df, transcripts_delta_path, insert_only=True):
    """
    Upsert transcripts keyed by episode_id.
    Required columns:
      - episode_id
      - transcript (string)
    If insert_only=True, we never overwrite an existing transcript.
    """
    now = F.current_timestamp()
    if "created_at" not in transcripts_df.columns:
        transcripts_df = transcripts_df.withColumn("created_at", now)
    transcripts_df = transcripts_df.withColumn("updated_at", now)

    _ensure_table(spark, transcripts_delta_path, transcripts_df)

    tgt = DeltaTable.forPath(spark, transcripts_delta_path)

    if insert_only:
        # Only insert rows whose episode_id is not present yet
        (tgt.alias("target")
            .merge(transcripts_df.alias("source"), "target.episode_id = source.episode_id")
            .whenNotMatchedInsertAll()
            .execute())
    else:
        # Allow overwrite if you ever need it
        (tgt.alias("target")
            .merge(transcripts_df.alias("source"), "target.episode_id = source.episode_id")
            .whenMatchedUpdate(set={
                "transcript": F.col("source.transcript"),
                "updated_at": F.col("source.updated_at"),
            })
            .whenNotMatchedInsertAll()
            .execute())