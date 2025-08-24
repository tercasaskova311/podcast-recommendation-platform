#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import pathlib
from datetime import datetime, timezone
from typing import List, Dict, Any

import pandas as pd
import pyarrow as pa
from util.delta_io import write_delta_overwrite
from config.settings import SAMPLE_EPISODES_JSON_PATH, DELTA_PATH_EPISODES, DELTA_PATH_TRANSCRIPTS


def _read_json_records(path: str) -> List[Dict[str, Any]]:
    p = pathlib.Path(path)
    if not p.exists():
        raise FileNotFoundError(f"JSON file not found: {p}")
    try:
        with p.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, list):
            return [x for x in obj if isinstance(x, dict)]
        if isinstance(obj, dict):
            return [obj]
    except json.JSONDecodeError:
        records = []
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if isinstance(rec, dict):
                    records.append(rec)
        if records:
            return records
    raise ValueError("Invalid JSON format (expected array, object, or NDJSON)")

def _derive_podcast_url(audio_url: str) -> str:
    if not audio_url or not isinstance(audio_url, str):
        return None
    m = re.match(r"^(https?://[^/]+)", audio_url.strip())
    return m.group(1) if m else None

# NOTE: we use int64 epoch millis for ts_ms to avoid Delta “TimestampWithoutTimezone” feature
ARROW_SCHEMA_EPISODES = pa.schema([
    pa.field("podcast_title",   pa.string()),
    pa.field("podcast_author",  pa.string()),
    pa.field("podcast_url",     pa.string()),
    pa.field("episode_title",   pa.string()),
    pa.field("description",     pa.string()),
    pa.field("audio_url",       pa.string()),
    pa.field("episode_id",      pa.int64()),
    pa.field("analyzed",        pa.bool_()),
    pa.field("failed",          pa.bool_()),
    pa.field("ts_ms",           pa.int64()),
])

ARROW_SCHEMA_TRANSCRIPTS = pa.schema([
    pa.field("episode_id",      pa.int64()),
    pa.field("transcript",      pa.string()),
])

def _build_frames(records: List[Dict[str, Any]]) -> (pd.DataFrame, pd.DataFrame):
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    rows_meta, rows_tr = [], []
    for r in records:
        episode_id = r.get("episode_id")
        if episode_id is None:
            continue

        description = r.get("description") or r.get("podcast_description")
        transcript  = r.get("transcript")  or r.get("text")

        rows_meta.append({
            "podcast_title":  r.get("podcast_title"),
            "podcast_author": r.get("podcast_author"),
            "podcast_url":    _derive_podcast_url(r.get("audio_url")),
            "episode_title":  r.get("episode_title"),
            "description":    description,
            "audio_url":      r.get("audio_url"),
            "episode_id":     int(episode_id),
            "analyzed":       False,
            "failed":         False,
            "ts_ms":          now_ms,
        })
        rows_tr.append({
            "episode_id": int(episode_id),
            "transcript": transcript,
        })

    meta_df = pd.DataFrame(rows_meta, columns=[f.name for f in ARROW_SCHEMA_EPISODES])
    tr_df   = pd.DataFrame(rows_tr,   columns=[f.name for f in ARROW_SCHEMA_TRANSCRIPTS])

    if not meta_df.empty:
        meta_df["episode_id"] = meta_df["episode_id"].astype("int64")
        meta_df["analyzed"]   = meta_df["analyzed"].astype("bool")
        meta_df["failed"]     = meta_df["failed"].astype("bool")
        meta_df["ts_ms"]      = meta_df["ts_ms"].astype("int64")

    if not tr_df.empty:
        tr_df["episode_id"] = tr_df["episode_id"].astype("int64")

    return meta_df, tr_df

def main():
    for var in ("SAMPLE_EPISODES_JSON_PATH", "DELTA_PATH_EPISODES", "DELTA_PATH_TRANSCRIPTS"):
        if not globals()[var]:
            raise ValueError(f"{var} is not set.")

    records = _read_json_records(SAMPLE_EPISODES_JSON_PATH)
    meta_df, tr_df = _build_frames(records)

    n_meta = write_delta_overwrite(DELTA_PATH_EPISODES, meta_df, ARROW_SCHEMA_EPISODES)
    n_tr   = write_delta_overwrite(DELTA_PATH_TRANSCRIPTS, tr_df, ARROW_SCHEMA_TRANSCRIPTS)

    print(f"Wrote {n_meta} rows → {DELTA_PATH_EPISODES}")
    print(f"Wrote {n_tr} rows → {DELTA_PATH_TRANSCRIPTS}")

if __name__ == "__main__":
    main()
