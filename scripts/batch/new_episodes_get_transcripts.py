# scripts/batch/new_episodes_transcript_download.py
"""
Batch pipeline (NO SPARK):
- Load retryable failed transcripts first (retry_count < 3)
- Poll Kafka for episode metadata (bounded batch)
- Upsert NEW metadata to Delta so retries can find audio_url next runs
- Process ONE episode at a time: transcribe -> upsert transcript -> commit Kafka offset for that eid
"""

import os
import json
import time
from typing import Any, Dict, List, Tuple, Set
from collections import defaultdict
from util.schemas import EPISODES_SCHEMA, TRANSCRIPTS_SCHEMA


from kafka import KafkaConsumer
from kafka.errors import CommitFailedError, RebalanceInProgressError
from kafka.structs import TopicPartition, OffsetAndMetadata
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
MAX_RECORDS = 5
POLL_TIMEOUT_MS = 5000
FAILED_EPISODES_MAX_RETRY = 5 #Reprocess max. 5 episodes at each run
POLL_LOOPS_MAX=5

# Kafka liveness (long transcription protection)
KAFKA_MAX_POLL_INTERVAL_MS=1800000 #30 min
KAFKA_SESSION_TIMEOUT_MS=45000
KAFKA_HEARTBEAT_INTERVAL_MS=3000

CONSUMER_GROUP = "episode-download-batch"
EPISODE_KEY = "episode_id"


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

    episode_id = payload.get(EPISODE_KEY)
    if episode_id is None and key_bytes is not None:
        try:
            episode_id = key_bytes.decode("utf-8")
        except Exception:
            episode_id = None

    return {
        "episode_id": str(episode_id) if episode_id is not None else None,
        "podcast_title": payload.get("podcast_title"),
        "podcast_author": payload.get("podcast_author"),
        "podcast_url": payload.get("podcast_url"),
        "episode_title": payload.get("episode_title"),
        "description": payload.get("description"),
        "audio_url": payload.get("audio_url"),
        "json_str": json.dumps(payload, ensure_ascii=False),
    }

def _update_highest(highest, records):
    for rec in records:
        tp, off = rec["tp"], rec["offset"]
        if tp not in highest or off > highest[tp]:
            highest[tp] = off

def _poll_once(consumer, max_records, poll_timeout_ms):
    msgs = consumer.poll(timeout_ms=poll_timeout_ms, max_records=max_records)
    out = []
    for tp, recs in msgs.items():
        for r in recs:
            out.append({"value": r.value, "key": r.key, "tp": tp, "offset": r.offset})
    return out

def poll_until_new(consumer, max_records, poll_timeout_ms, max_attempts=3, existing_ids: Set[str] = None):
    """
    Returns: (new_eps, ep_offsets, polled_highest)
      - new_eps: list of parsed episode dicts that are NEW vs existing_ids
      - ep_offsets: {episode_id -> (TopicPartition, offset)} for items seen in this run
      - polled_highest: {TopicPartition -> highest_offset_seen} for blanket commit if needed
    """
    if existing_ids is None:
        existing_ids = set()

    parsed_rows: list[dict] = []
    ep_offsets: dict[str, tuple[TopicPartition, int]] = {}
    polled_highest: dict[TopicPartition, int] = {}

    for _ in range(max_attempts):
        records = _poll_once(consumer, max_records, poll_timeout_ms)
        if not records:
            # nothing this round; keep trying a bit (heartbeat to avoid rebalance)
            consumer.poll(0)
            continue

        _update_highest(polled_highest, records)

        # parse
        for rec in records:
            row = parse_episode_record(rec["value"], rec["key"])
            if row.get("episode_id") and row.get("audio_url"):
                eid = row["episode_id"]
                parsed_rows.append(row)
                prev = ep_offsets.get(eid)
                if (prev is None) or (rec["offset"] > prev[1]):
                    ep_offsets[eid] = (rec["tp"], rec["offset"])

        # check "new" so far
        new_eps = [r for r in parsed_rows if r["episode_id"] not in existing_ids]
        if new_eps:
            return new_eps, ep_offsets, polled_highest

        # keep looping if we still haven't seen anything new
        consumer.poll(0)

    # final pass after loop
    new_eps = [r for r in parsed_rows if r["episode_id"] not in existing_ids]
    return new_eps, ep_offsets, polled_highest


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

def commit_offset_for_eid(
    consumer: KafkaConsumer,
    eid: str,
    ep_offsets: Dict[str, Tuple[TopicPartition, int]],
    max_retries: int = 5,
    backoff_sec: float = 0.5,
):
    tp_off = ep_offsets.get(eid)
    if not tp_off:
        return
    tp, off = tp_off
    payload = {tp: OffsetAndMetadata(off + 1, None, -1)}
    attempt = 0
    while True:
        try:
            consumer.poll(0)
            consumer.commit(payload)
            return
        except (CommitFailedError, RebalanceInProgressError):
            attempt += 1
            if attempt > max_retries:
                # We already persisted to Delta idempotently; worst case we re-read and short-circuit next run.
                raise
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
    if tdf.empty or "failed" not in tdf.columns or "episode_id" not in tdf.columns:
        return []
    
        # Drop schema/placeholder rows: require a real episode_id
    tdf = tdf[tdf["episode_id"].notna()]
    tdf["episode_id"] = tdf["episode_id"].astype(str).str.strip()
    tdf = tdf[tdf["episode_id"] != ""]
    if tdf.empty:
        return []

    # Normalize retry_count safely: None, "", "integer", etc. -> 0
    import pandas as pd
    if "retry_count" not in tdf.columns:
        tdf["retry_count"] = 0
    tdf["retry_count"] = (
        pd.to_numeric(tdf["retry_count"], errors="coerce")
          .fillna(0)
          .astype(int)
    )

    retryable = tdf[(tdf["failed"] == True) & (tdf["retry_count"] < 3)].copy()

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

    ensure_table(DELTA_PATH_EPISODES, EPISODES_SCHEMA)
    ensure_table(DELTA_PATH_TRANSCRIPTS, TRANSCRIPTS_SCHEMA)

    # 1) Load retries
    retry_queue = load_retry_queue()
    print(f"Retryable failed episodes: {len(retry_queue)}")

    # 2) Create consumer
    consumer = KafkaConsumer(
        TOPIC_EPISODE_METADATA,
        bootstrap_servers=KAFKA_URL,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=MAX_RECORDS,
        max_poll_interval_ms=KAFKA_MAX_POLL_INTERVAL_MS,
        session_timeout_ms=KAFKA_SESSION_TIMEOUT_MS,
        heartbeat_interval_ms=KAFKA_HEARTBEAT_INTERVAL_MS,
    )

    existing_ids = set(str(x) for x in get_existing_ids(DELTA_PATH_EPISODES, EPISODE_KEY))

    # 3) Poll until we find at least 1 new episode
    new_eps, ep_offsets, polled_highest = poll_until_new(
        consumer, MAX_RECORDS, POLL_TIMEOUT_MS, FAILED_EPISODES_MAX_RETRY, existing_ids
    )

    # Upsert metadata for NEW
    if new_eps:
        upsert_delta(DELTA_PATH_EPISODES, new_eps, key=EPISODE_KEY, schema=EPISODES_SCHEMA)

    if not retry_queue and not new_eps:
        print("Nothing to process (no retries, no new episodes).")
        consumer.close()
        return

    print(f"Episodes to process (retries + new) ({len(retry_queue)} + {len(new_eps)}): {len(retry_queue)+len(new_eps)}")

    # 4) Build worklist
    seen: Set[str] = set()
    work: List[Dict[str, Any]] = []
    for ep in retry_queue[:FAILED_EPISODES_MAX_RETRY]:
        if ep["episode_id"] not in seen:
            work.append(ep)
            seen.add(ep["episode_id"])
    for ep in new_eps:
        if ep["episode_id"] not in seen:
            ep["_prev_retry_count"] = 0
            work.append(ep)
            seen.add(ep["episode_id"])

    # 5) Process sequentially
    for ep in work:
        eid = str(ep["episode_id"])
        prev_retry = int(ep.get("_prev_retry_count") or 0)

        # process_batch([ep]) yields exactly one result
        try:
            result = next(process_batch([ep]))
        except StopIteration:
            result = None
        except Exception as e:
            result = {
                "episode_id": eid, "transcript": None, "failed": True,
                "error": f"process_batch error: {e}", "duration": None, "language": None,
            }

        if result is None:
            result = {
                "episode_id": eid, "transcript": None, "failed": True,
                "error": "no result", "duration": None, "language": None,
            }

        row = {
            "episode_id": str(eid),
            "transcript": (result.get("transcript") or None),
            "failed": bool(result.get("failed", False)),
            "error": (result.get("error") or None),
            "duration": float(result.get("duration") or 0.0) if result.get("duration") is not None else None,
            "language": (result.get("language") or None),
            "analyzed": False,
            "ingest_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "retry_count": min(prev_retry + 1, 3) if result.get("failed") else prev_retry
        }
        print(row)
        upsert_delta(DELTA_PATH_TRANSCRIPTS, [row], key=EPISODE_KEY, schema=TRANSCRIPTS_SCHEMA)

        # Commit the offset for this eid if it was part of the Kafka poll
        if eid in ep_offsets:
            consumer.poll(0)  # keep membership fresh
            commit_offset_for_eid(consumer, eid, ep_offsets)

        consumer.poll(0)  # heartbeat between episodes

    # 6) Blanket commit last poll if any
    if polled_highest:
        commit_offsets_map(consumer, polled_highest)

    consumer.close()
    print(f"Pipeline completed in {time.time() - start:.1f}s")

if __name__ == "__main__":
    run_pipeline()
