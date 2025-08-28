#!/usr/bin/env python3
"""
seed_delta_events.py
Purpose:
  Append synthetic user events into a Delta Lake table so your Streamlit/DuckDB reader can test.
Requires:
  pip install deltalake pandas pyarrow
Usage:
  python seed_delta_events.py --path s3://bucket/path/to/delta/events --minutes 30 --users 100 --events-per-user 10 --mode append
  # Or local:
  python seed_delta_events.py --path ./_delta/events --minutes 15 --users 10 --events-per-user 20 --mode overwrite
Environment:
  DELTA_EVENTS_PATH   (fallback if --path not provided)
  AWS_*               (if writing to S3)
Schema:
  ts (timestamp, UTC), user_id (string), episode_id (string), event (string), rating (int|nullable)
Notes:
  - This script uses delta-rs (deltalake) to WRITE; DuckDB will READ.
"""
import os
import argparse
from datetime import datetime, timedelta, timezone
import random
import pandas as pd

try:
    from deltalake import write_deltalake, DeltaTable
except Exception as e:
    raise SystemExit("This script requires 'deltalake'. Install with: pip install deltalake pandas pyarrow") from e

EVENTS = ["like", "complete", "pause", "rate", "skip"]

def synth_df(minutes: int, n_users: int, events_per_user: int, seed: int) -> pd.DataFrame:
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)
    rows = []
    # user and episode pools
    users = [f"u{uid:05d}" for uid in range(1, n_users + 1)]
    episodes = [f"e{eid:05d}" for eid in range(1, max(200, n_users * 5) + 1)]
    for u in users:
        for _ in range(events_per_user):
            ev = rng.choice(EVENTS)
            ts = now - timedelta(minutes=rng.randint(0, minutes), seconds=rng.randint(0, 59))
            ep = rng.choice(episodes)
            rating = None
            if ev == "rate":
                rating = rng.randint(1, 5)
            rows.append({
                "ts": ts,
                "user_id": u,
                "episode_id": ep,
                "event": ev,
                "rating": rating,
            })
    df = pd.DataFrame(rows)
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", type=str, default=os.getenv("DELTA_EVENTS_PATH", "./_delta/events"))
    ap.add_argument("--minutes", type=int, default=30)
    ap.add_argument("--users", type=int, default=50)
    ap.add_argument("--events-per-user", type=int, default=10)
    ap.add_argument("--seed", type=int, default=123)
    ap.add_argument("--mode", choices=["append", "overwrite"], default="append", help="Delta write mode")
    args = ap.parse_args()

    df = synth_df(args.minutes, args.users, args.events_per_user, args.seed)

    # Write to delta (create if missing)
    write_deltalake(
        args.path,
        df,
        mode=args.mode,
        # schema is inferred; if you later evolve, you can set schema_mode='merge'
        schema_mode="merge",
        # partitions could be added later, e.g., partition_by=["date"]
    )
    print(f"Wrote {len(df)} rows to Delta table at: {args.path}")
    try:
        dt = DeltaTable(args.path)
        print("Current version:", dt.version())
        print("Files:", len(dt.files()))
    except Exception:
        pass

if __name__ == "__main__":
    main()
