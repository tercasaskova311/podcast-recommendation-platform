# scripts/batch/new_episodes_transcript_download.py
"""
Batch pipeline (NO SPARK):
1) Read episode metadata from Kafka (bounded batch)
2) Parse & keep only NEW episodes vs Delta `episodes` table
3) Download + transcribe one episode at a time (via util.transcription.process_batch([ep]))
4) Upsert episodes & transcripts to Delta (delta-rs) idempotently by episode_id
5) Commit Kafka offset for that episode immediately after successful persistence
6) Also retry previously failed transcripts (retry_count < 3) before new items
"""

import os
import json
import time
from typing import Any, Dict, List, Tuple, Optional, Set
from collections import defaultdict

from kafka import KafkaConsumer
from kafka.errors import CommitFailedError, RebalanceInProgressError
from kafka.structs import TopicPartition, OffsetAndMetadata

# NEW: read Delta to assemble retry queue
from deltalake import DeltaTable

from util.delta_io import ensure_table, upsert_delta, get_existing_ids
from util.transcription import process_batch

# ---- Project config ----
from config.settings import (
    KAFKA_URL,
    TOPIC_EPISODE_METADATA,
    DELTA_PATH_EPISODES,
    DELTA_PATH_TRANSCRIPTS,
)

# ---- Tunables via ENV ----
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "20"))
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "5000"))

CONSUMER_GROUP = "episode-download-batch"
EPISODE_KEY = "episode_id"

# Kafka liveness (long transcription protection)
KAFKA_MAX_POLL_INTERVAL_MS = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "1800000"))  # 30m
KAFKA_SESSION_TIMEOUT_MS   = int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "45000"))      # 45s
KAFKA_HEARTBEAT_INTERVAL_MS= int(os.getenv("KAFKA_HEARTBEAT_INTERVAL_MS", "3000"))    # 3s


# =========================
# Kafka helpers
# =========================
def parse_episode_record(value: Any, key_bytes: bytes) -> Dict[str, Any]:
    """
    Accepts raw Kafka value (bytes/str/dict) and key (bytes).
    Returns a normalized row with `episode_id` (str), metadata fields, and json_str.
    """
    # Decode payload
    if isinstance(value, (bytes, bytearray)):
        try:
            payload = json.loads(value.decode("utf-8"))
        except Exception:
            payload = {}
    elif isinstance(value, str):
        try:
            payload = json.loads(value)
        except Exception:
            payload = {}
    elif isinstance(value, dict):
        payload = value
    else:
        payload = {}

    # episode_id: prefer payload; fallback to Kafka key
    episode_id = payload.get(EPISODE_KEY)
    if episode_id is None and key_bytes is not None:
        try:
            episode_id = key_bytes.decode("utf-8")
        except Exception:
            episode_id = None

    # Normalize to string
    eid_str = str(episode_id) if episode_id is not None else None

    return {
        "episode_id": eid_str,
        "podcast_title": payload.get("podcast_title"),
        "podcast_author": payload.get("podcast_author"),
        "podcast_url": payload.get("podcast_url"),
        "episode_title": payload.get("episode_title"),
        "description": payload.get("description"),
        "audio_url": payload.get("audio_url"),
        "json_str": json.dumps(payload, ensure_ascii=False),
    }

def read_episode_metadata_batch(
    kafka_url: str,
    topic: str,
    group_id: str,
    max_records: int,
    poll_timeout_ms: int,
):
    """
    Read a bounded batch from Kafka. Returns (consumer, records),
    where records = list of dicts:
      {"value": raw_value, "key": raw_key, "tp": TopicPartition, "offset": int}
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_url,
        group_id=group_id,
        enable_auto_commit=False,
        value_deserializer=lambda v: v,
        key_deserializer=lambda k: k,
        auto_offset_reset="earliest",
        max_poll_records=max_records,
        # keep membership during long processing:
        max_poll_interval_ms=KAFKA_MAX_POLL_INTERVAL_MS,
        session_timeout_ms=KAFKA_SESSION_TIMEOUT_MS,
        heartbeat_interval_ms=KAFKA_HEARTBEAT_INTERVAL_MS,
    )
    msgs = consumer.poll(timeout_ms=poll_timeout_ms, max_records=max_records)
    records: List[Dict[str, Any]] = []
    for tp, recs in msgs.items():
        for r in recs:
            records.append({"value": r.value, "key": r.key, "tp": tp, "offset": r.offset})
    return consumer, records

def commit_offsets_map(consumer: KafkaConsumer, tp_to_offset_inclusive: Dict[TopicPartition, int]) -> None:
    if not tp_to_offset_inclusive:
        return
    commit_payload = {
        tp: OffsetAndMetadata(off + 1, None, -1)
        for tp, off in tp_to_offset_inclusive.items()
        if off >= 0
    }
    if not commit_payload:
        return
    try:
        consumer.commit(commit_payload)
    except CommitFailedError:
        # likely rebalance; rejoin and retry once
        consumer.poll(0)  # send heartbeat / rejoin if needed
        consumer.commit(commit_payload)

def commit_all_polled(consumer: KafkaConsumer, records: List[Dict[str, Any]]) -> None:
    """
    When we decide to skip processing (e.g., no new episodes), advance the group
    cursor past everything we just polled so we don't see it again next run.
    """
    highest: Dict[TopicPartition, int] = {}
    for rec in records:
        tp = rec["tp"]
        off = rec["offset"]
        if tp not in highest or off > highest[tp]:
            highest[tp] = off
    commit_offsets_map(consumer, highest)

def commit_offset_for_eid(consumer, eid, ep_offsets, max_retries: int = 5, backoff_sec: float = 0.5):
    """
    Commit the offset for a specific episode_id that we just persisted.
    """
    tp_off = ep_offsets.get(eid)
    if not tp_off:
        return
    tp, off = tp_off
    payload = {tp: OffsetAndMetadata(off + 1, None, -1)}
    attempt = 0
    while True:
        try:
            consumer.commit(payload)
            return
        except (CommitFailedError, RebalanceInProgressError):
            attempt += 1
            if attempt > max_retries:
                # We've already persisted to Delta idempotently; worst case we'll re-read and short-circuit next run
                raise
            # Rejoin/heartbeat and back off briefly
            consumer.poll(0)
            time.sleep(backoff_sec * attempt)

# =========================
# Retry queue helpers (Delta)
# =========================
def _delta_exists(path: str) -> bool:
    return os.path.exists(os.path.join(path, "_delta_log"))

def safe_read_df(path: str):
    try:
        return DeltaTable(path).to_pandas()
    except Exception:
        import pandas as pd
        return pd.DataFrame()

def load_retry_queue() -> List[Dict[str, Any]]:
    """
    Load transcripts with failed==True and retry_count < 3, join episodes to get audio_url.
    Returns episode dicts compatible with parse_episode_record output.
    """
    if not (_delta_exists(DELTA_PATH_TRANSCRIPTS) and _delta_exists(DELTA_PATH_EPISODES)):
        return []

    tdf = safe_read_df(DELTA_PATH_TRANSCRIPTS)
    if tdf.empty:
        return []

    if "failed" not in tdf.columns:
        return []
    if "retry_count" not in tdf.columns:
        tdf["retry_count"] = 0

    retryable = tdf[(tdf["failed"] == True) & (tdf["retry_count"].fillna(0) < 3)]
    if retryable.empty:
        return []

    edf = safe_read_df(DELTA_PATH_EPISODES)
    if edf.empty:
        return []

    retryable["episode_id"] = retryable["episode_id"].astype(str)
    edf["episode_id"] = edf["episode_id"].astype(str)

    merged = retryable.merge(edf, on="episode_id", how="left", suffixes=("_t", "_e"))
    out: List[Dict[str, Any]] = []
    for _, row in merged.iterrows():
        audio_url = row.get("audio_url")
        if isinstance(audio_url, str) and audio_url:
            out.append({
                "episode_id": row["episode_id"],
                "audio_url": audio_url,
                "podcast_title": row.get("podcast_title"),
                "podcast_author": row.get("podcast_author"),
                "podcast_url": row.get("podcast_url"),
                "episode_title": row.get("episode_title"),
                "description": row.get("description"),
                "_prev_retry_count": int(row.get("retry_count") or 0),
            })
    return out


# =========================
# Orchestrator (per-episode)
# =========================
def run_pipeline():
    start = time.time()


    # 1) Ensure tables and filter NEW vs episodes table
    ensure_table(
        DELTA_PATH_EPISODES,
        {
            "episode_id": "",
            "podcast_title": None,
            "podcast_author": None,
            "podcast_url": None,
            "episode_title": None,
            "description": None,
            "audio_url": None,
            "json_str": None,
        },
    )
    ensure_table(
        DELTA_PATH_TRANSCRIPTS,
        {
            "episode_id": "",
            "transcript": None,
            "failed": False,
            "error": None,
            "duration": None,
            "language": None,
            "analyzed": False,
            "ingest_ts": None,
            "retry_count": 0,
        },
    )

    # 2) Load failed episodes to try to download them again
    retry_queue = load_retry_queue()
    print(f"There are {retry_queue} failed episoes to retry")

    # 3) Read bounded batch
    consumer, records = read_episode_metadata_batch(
        kafka_url=KAFKA_URL,
        topic=TOPIC_EPISODE_METADATA,
        group_id=CONSUMER_GROUP,
        max_records=MAX_RECORDS,
        poll_timeout_ms=POLL_TIMEOUT_MS,
    )

    new_eps = []

    if not records and not retry_queue:
        print("Nothing to process (no retries, no new episodes).")
        consumer.close()
        return
    elif not retry_queue:
        # Parse records, track offsets per episode
        parsed_rows: List[Dict[str, Any]] = []
        ep_offsets: Dict[str, Tuple[TopicPartition, int]] = {}  # episode_id -> (tp, offset)
        
        for rec in records:
            row = parse_episode_record(rec["value"], rec["key"])
            if row.get("episode_id") and row.get("audio_url"):
                eid = row["episode_id"]
                parsed_rows.append(row)
                # Keep the HIGHEST offset we saw for this episode_id (dedupe within batch)
                prev = ep_offsets.get(eid)
                if (prev is None) or (rec["offset"] > prev[1]):
                    ep_offsets[eid] = (rec["tp"], rec["offset"])
        
        #Quit if not usable data
        if not parsed_rows:
            print("No usable rows in batch. Committing polled offsets to advance the group cursor.")
            commit_all_polled(consumer, records)
            consumer.close()
            return
        
        existing_ids = set(str(x) for x in get_existing_ids(DELTA_PATH_EPISODES, EPISODE_KEY))
        new_eps = [r for r in parsed_rows if r[EPISODE_KEY] not in existing_ids]

        # Upsert metadata for NEW in one shot (so subsequent retries know the audio_url)
        if not new_eps:
            print("Fetched already store episodes. Move on")
            commit_all_polled(consumer, records)
            consumer.close()
            return
        else:
            upsert_delta(DELTA_PATH_EPISODES, new_eps, key=EPISODE_KEY)

        print(f"Fetched {len(parsed_rows)} episode, just {len(new_eps)} of them are new.")

    
    # 4) Build work list: retries first, then new items
    seen: Set[str] = set()
    work: List[Dict[str, Any]] = []
    for ep in retry_queue:
        eid = ep["episode_id"]
        if eid not in seen:
            work.append(ep)
            seen.add(eid)
    for ep in new_eps:
        eid = ep["episode_id"]
        if eid not in seen:
            ep["_prev_retry_count"] = 0
            work.append(ep)
            seen.add(eid)

    print(f"Episodes to process (retries + new) ({len(retry_queue)} + {len(new_eps)}): {len(work)}")

    # 4) Process ONE BY ONE: transcribe -> upsert -> commit offset for that eid
    for ep in work:
        eid = str(ep["episode_id"])
        prev_retry = int(ep.get("_prev_retry_count") or 0)

        # Call your existing util generator for a single episode
        # process_batch([ep]) yields exactly one result dict
        result = None
        try:
            for res in process_batch([ep]):
                result = res
                break
        except Exception as e:
            result = {
                "episode_id": eid,
                "transcript": None,
                "failed": True,
                "error": f"process_batch error: {e}",
                "duration": None,
                "language": None,
            }

        if result is None:
            result = {
                "episode_id": eid,
                "transcript": None,
                "failed": True,
                "error": "no result",
                "duration": None,
                "language": None,
            }

        # Build transcript row (increment retry_count only on failure, capped at 3)
        row = {
            "episode_id": eid,
            "transcript": result.get("transcript"),
            "failed": bool(result.get("failed", False)),
            "error": result.get("error"),
            "duration": result.get("duration"),
            "language": result.get("language"),
            "analyzed": False,
            "ingest_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "retry_count": min(prev_retry + 1, 3) if result.get("failed") else prev_retry,
        }
        print(row)

        # Persist immediately
        upsert_delta(DELTA_PATH_TRANSCRIPTS, [row], key=EPISODE_KEY)

        # Heartbeat to keep group membership
        consumer.poll(0)
    
    commit_all_polled(consumer, records)

    # 5) Done
    consumer.close()
    print(f"Pipeline completed in {time.time() - start:.1f}s")


if __name__ == "__main__":
    run_pipeline()