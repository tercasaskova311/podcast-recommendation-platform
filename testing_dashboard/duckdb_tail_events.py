#!/usr/bin/env python3
"""
duckdb_tail_events.py
Purpose:
  Validate you can READ your Delta events with DuckDB (outside Streamlit).
Requires:
  pip install duckdb pandas
Usage:
  python duckdb_tail_events.py --path ./_delta/events --minutes 15 --limit 20
  python duckdb_tail_events.py --path s3://bucket/path --minutes 10 --limit 5
Notes:
  - If using S3, configure AWS_* env vars. The script will INSTALL/LOAD delta and httpfs.
"""
import os
import argparse
import duckdb
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", type=str, required=True)
    ap.add_argument("--minutes", type=int, default=10)
    ap.add_argument("--limit", type=int, default=20)
    args = ap.parse_args()

    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    if args.path.startswith("s3://"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("SET s3_region = $r", {"r": os.getenv("AWS_REGION", "eu-west-1")})
        con.execute("SET s3_access_key_id = $a", {"a": os.getenv("AWS_ACCESS_KEY_ID", "")})
        con.execute("SET s3_secret_access_key = $s", {"s": os.getenv("AWS_SECRET_ACCESS_KEY", "")})
        con.execute("SET s3_session_token = $t", {"t": os.getenv("AWS_SESSION_TOKEN", "")})

    df = con.execute(
        """
        WITH src AS (
          SELECT try_cast(ts AS TIMESTAMP) AS ts, user_id, episode_id, event, rating
          FROM delta_scan(?)
        )
        SELECT *
        FROM src
        WHERE ts >= now() - (? * INTERVAL 1 MINUTE)
        ORDER BY ts DESC
        LIMIT ?
        """,
        [args.path, int(args.minutes), int(args.limit)],
    ).df()

    print(df.head(args.limit).to_string(index=False))
    print(f"\nRows returned: {len(df)}")

if __name__ == "__main__":
    main()
