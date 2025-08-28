#!/usr/bin/env python3
"""
seed_delta_episodes.py
Purpose:
  Create/refresh a Delta Lake table with EPISODE METADATA (titles, podcast info),
  so your Streamlit app can join episode titles instead of showing raw IDs.

Two ways to build the table:
  A) From existing EVENTS (recommended): scan a Delta events table for distinct episode_id
     and generate simple titles for exactly those IDs.
  B) From a synthetic range: generate N episodes like e00001..e00100.

Requires:
  pip install deltalake duckdb pandas pyarrow

Usage examples:
  # A) Build from existing events (reads distinct episode_id via DuckDB)
  python seed_delta_episodes.py --episodes-path ./_delta/episodes --events-path ./_delta/events --mode overwrite

  # B) Synthetic (no events table yet)
  python seed_delta_episodes.py --episodes-path ./_delta/episodes --count 200 --mode overwrite

Environment:
  DELTA_PATH_EPISODES  (default if --episodes-path not provided)
  DELTA_EVENTS_PATH    (optional, used if --events-path not provided)
  AWS_*                (if paths are s3://)
"""

import os
import argparse
import random
import pandas as pd

try:
    from deltalake import write_deltalake
except Exception as e:
    raise SystemExit("This script requires 'deltalake'. Install with: pip install deltalake pandas pyarrow") from e

# We use DuckDB only when reading distinct episode_ids from an events table.
def _distinct_episode_ids(events_path: str) -> list[str]:
    import duckdb  # local import so it's optional otherwise
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    if events_path.startswith("s3://"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("SET s3_region = $r", {"r": os.getenv("AWS_REGION", "eu-west-1")})
        con.execute("SET s3_access_key_id = $a", {"a": os.getenv("AWS_ACCESS_KEY_ID", "")})
        con.execute("SET s3_secret_access_key = $s", {"s": os.getenv("AWS_SECRET_ACCESS_KEY", "")})
        con.execute("SET s3_session_token = $t", {"t": os.getenv("AWS_SESSION_TOKEN", "")})
    df = con.execute(
        """
        SELECT DISTINCT CAST(episode_id AS VARCHAR) AS episode_id
        FROM delta_scan(?)
        WHERE episode_id IS NOT NULL
        """,
        [events_path],
    ).df()
    ids = sorted(df["episode_id"].dropna().astype(str).unique().tolist())
    return ids

def _ensure_local_dir(uri: str):
    # Create local dirs if writing to filesystem; not for s3:// or gs://
    if not uri.startswith(("s3://", "gs://")):
        os.makedirs(uri, exist_ok=True)

def _synth_titles(ids: list[str]) -> pd.DataFrame:
    rows = []
    rng = random.Random(123)
    shows = [
        ("Deep Data Daily", "Ada Lovelace"),
        ("Pod of Casts", "Alan Turing"),
        ("Stats & Stories", "Florence Nightingale"),
        ("Vectors & Voices", "Grace Hopper"),
    ]
    for eid in ids:
        show, author = rng.choice(shows)
        num = eid[-3:] if (isinstance(eid, str) and len(eid) > 1 and eid[0].lower() == 'e' and eid[1:].isdigit()) else str(rng.randint(1, 999))
        title = f"{show} – Ep {num}"
        rows.append({
            "episode_id": str(eid),
            "episode_title": title,
            "podcast_title": show,
            "podcast_author": author,
            "description": f"Synthetic title for {eid}",
        })
    return pd.DataFrame(rows)

def _synth_range(n: int) -> list[str]:
    return [f"e{idx:05d}" for idx in range(1, n + 1)]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--episodes-path", type=str, default=os.getenv("DELTA_PATH_EPISODES", "./_delta/episodes"))
    ap.add_argument("--events-path", type=str, default=os.getenv("DELTA_EVENTS_PATH"))  # optional
    ap.add_argument("--count", type=int, default=200, help="If no events-path, generate this many IDs")
    ap.add_argument("--mode", choices=["append", "overwrite"], default="overwrite", help="Delta write mode")
    args = ap.parse_args()

    # Decide ID source
    if args.events_path:
        try:
            ids = _distinct_episode_ids(args.events_path)
        except Exception as e:
            raise SystemExit(f"Failed reading distinct episode_id from events at {args.events_path}: {e}")
        if not ids:
            # fallback to synthetic if events table exists but empty
            ids = _synth_range(args.count)
    else:
        ids = _synth_range(args.count)

    df = _synth_titles(ids)

    _ensure_local_dir(args.episodes_path)
    write_deltalake(
        args.episodes_path,
        df,
        mode=args.mode,
        schema_mode="merge",
    )
    print(f"Wrote {len(df)} episode rows to Delta: {args.episodes_path}")

if __name__ == "__main__":
    main()
