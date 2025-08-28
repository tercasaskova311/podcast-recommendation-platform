#!/usr/bin/env python3
"""
seed_final_snapshot.py
Purpose:
  Seed (or refresh) the MongoDB "final recommendations" snapshot that your Streamlit app reads.
  The script writes documents shaped like:
    { user_id, recommended_episode_id, score, generated_at }
Usage:
  python seed_final_snapshot.py --n-users 200 --overwrite
Environment:
  - Either provide config.settings (MONGO_URI, MONGO_DB, MONGO_COLLECTION_FINAL_RECS)
    OR set ENV vars: MONGO_URI, MONGO_DB, MONGO_COLLECTION_FINAL_RECS
Notes:
  - `generated_at` is stored as timezone-aware UTC datetime.
  - Scores are floats in [0, 1] by default (use --score-max to change the ceiling).
"""

import os
import argparse
import random
from datetime import datetime, timezone
from typing import List, Dict

try:
    # Prefer your project's config module if present
    from config.settings import MONGO_URI, MONGO_DB, MONGO_COLLECTION_FINAL_RECS
except Exception:
    MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    MONGO_DB = os.environ.get("MONGO_DB", "recs_db")
    MONGO_COLLECTION_FINAL_RECS = os.environ.get("MONGO_COLLECTION_FINAL_RECS", "final_recommendations")

from pymongo import MongoClient, ReplaceOne

def make_docs(n_users: int, episodes_pool: int, score_max: float, seed: int) -> List[Dict]:
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)
    docs = []
    for i in range(1, n_users + 1):
        user_id = f"u{i:05d}"
        # one recommended episode per user (you can increase fanout with --episodes-per-user)
        episode_id = f"e{rng.randint(1, episodes_pool):05d}"
        score = round(rng.random() * score_max, 3)
        docs.append({
            "user_id": user_id,
            "recommended_episode_id": episode_id,
            "score": score,
            "generated_at": now,
        })
    return docs

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-users", type=int, default=200, help="How many users to seed")
    ap.add_argument("--episodes-pool", type=int, default=1000, help="Unique episode IDs pool size")
    ap.add_argument("--score-max", type=float, default=1.0, help="Max possible score (inclusive)")
    ap.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    ap.add_argument("--overwrite", action="store_true", help="Drop the collection before inserting")
    args = ap.parse_args()

    client = MongoClient(MONGO_URI, tz_aware=True)
    col = client[MONGO_DB][MONGO_COLLECTION_FINAL_RECS]

    if args.overwrite:
        col.drop()

    docs = make_docs(args.n_users, args.episodes_pool, args.score_max, args.seed)

    # Upsert per user_id (idempotent refresh), or bulk insert if empty
    ops = [ReplaceOne({"user_id": d["user_id"]}, d, upsert=True) for d in docs]
    result = col.bulk_write(ops, ordered=False)

    # Index (helpful for your Streamlit filters)
    col.create_index("user_id")
    col.create_index("score")

    print({
        "mongo_uri": MONGO_URI,
        "db": MONGO_DB,
        "collection": MONGO_COLLECTION_FINAL_RECS,
        "matched": result.matched_count,
        "modified": result.modified_count,
        "upserted": len(result.upserted_ids or {}),
        "total_docs": col.estimated_document_count(),
    })

if __name__ == "__main__":
    main()
