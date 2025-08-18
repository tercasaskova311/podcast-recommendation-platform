# -*- coding: utf-8 -*-
import os
import datetime
from typing import Dict, Any, List, Optional

from pyspark.sql import SparkSession, functions as F, types as T

# ========= CONFIG =========
KAFKA_BOOTSTRAP = os.getenv("KAFKA_URL", "kafka1:9092")
KAFKA_TOPIC = os.getenv("TOPIC_USER_EVENTS", "user-events")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/data_lake/checkpoints/user_events_stream")
WATERMARK = os.getenv("WATERMARK_MIN", "10 minutes")

# Mongo
MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "podcasts")
MONGO_EPISODES = os.getenv("MONGO_COLL_EPISODES", "episodes")
MONGO_USERS = os.getenv("MONGO_COLL_USERS", "users")

# Recommendation knobs (simple, transparent)
W_FRESHNESS_DAYS_HALF_LIFE = float(os.getenv("FRESHNESS_HALF_LIFE_DAYS", "14"))
W_OVERLAP = float(os.getenv("W_OVERLAP", "0.6"))
W_PREF = float(os.getenv("W_PREF", "0.3"))
W_FRESH = float(os.getenv("W_FRESH", "0.1"))
W_SCORE = float(os.getenv("W_SCORE", "0.7"))
W_MATCH = float(os.getenv("W_MATCH", "0.3"))
CANDIDATES_LIMIT = int(os.getenv("CANDIDATES_LIMIT", "100"))
RECENT_RECS_CAP = int(os.getenv("RECENT_RECS_CAP", "10"))
LISTENED_CAP = int(os.getenv("LISTENED_CAP", "500"))

# Appreciation score weights
W_COMPLETIONS = float(os.getenv("W_COMPLETIONS", "1.5"))
W_LIKES = float(os.getenv("W_LIKES", "1.2"))
W_PLAYS = float(os.getenv("W_PLAYS", "0.6"))
W_RATING = float(os.getenv("W_RATING", "0.8"))
W_EARLY_SKIPS = float(os.getenv("W_EARLY_SKIPS", "1.0"))
W_SENTIMENT_POS = float(os.getenv("W_SENTIMENT_POS", "0.3"))

# ========= HELPERS (driver-side, used inside foreachBatch) =========

def _now_utc():
    return datetime.datetime.utcnow()

def _freshness_boost(published_at: Optional[datetime.datetime]) -> float:
    if not published_at:
        return 0.0
    age_days = max((_now_utc() - published_at).days, 0)
    # simple exponential decay mapped to [0..1]
    import math
    lam = math.log(2.0) / max(W_FRESHNESS_DAYS_HALF_LIFE, 1e-6)
    return math.exp(-lam * age_days)

def _normalize_score(score: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.5
    return max(0.0, min(1.0, (score - lo) / (hi - lo)))

def _jaccard(a: List[str], b: List[str]) -> float:
    if not a or not b:
        return 0.0
    A, B = set(a), set(b)
    return 0.0 if not A or not B else len(A & B) / float(len(A | B))

def _compute_episode_score(stats: Dict[str, int], sentiment: float) -> float:
    s = stats or {}
    rating_cnt = s.get("rating_cnt", 0)
    rating_avg = (s.get("rating_sum", 0.0) / rating_cnt) if rating_cnt > 0 else 0.0
    sentiment_pos = max(sentiment or 0.0, 0.0)
    return (
        W_COMPLETIONS * s.get("completions", 0)
        + W_LIKES * s.get("likes", 0)
        + W_PLAYS * s.get("plays", 0)
        + W_RATING * rating_avg * rating_cnt
        - W_EARLY_SKIPS * s.get("early_skips", 0)
        + W_SENTIMENT_POS * sentiment_pos
    )

def _choose_next_rec(episodes_coll, user_doc: Dict[str, Any], current_ep: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    1) fetch top episodes by score for primary topic (not listened, not recent)
    2) re-rank with overlap + user topic pref + freshness
    """
    if not current_ep:
        return None
    topics = current_ep.get("topics", []) or []
    if not topics:
        return None
    primary = topics[0]

    listened_map = (user_doc or {}).get("listened", {}) or {}
    recent_recs = (user_doc or {}).get("recent_recs", []) or []
    exclude_ids = set(list(listened_map.keys())[:LISTENED_CAP]) | set(recent_recs) | {current_ep["episode_id"]}

    # Mongo query: same topic, not in exclude, top scored
    cursor = episodes_coll.find(
        {"topics": primary, "episode_id": {"$nin": list(exclude_ids)}},
        {"episode_id": 1, "topics": 1, "score": 1, "podcast_id": 1, "published_at": 1}
    ).sort("score", -1).limit(CANDIDATES_LIMIT)

    # Estimate dynamic normalization bounds from first/last scores in cursor list
    cands = list(cursor)
    if not cands:
        return None

    # dynamic normalization bounds
    scores = [float(c.get("score", 0.0)) for c in cands]
    lo, hi = (min(scores), max(scores)) if scores else (0.0, 1.0)

    best, best_rs = None, -1.0
    user_prefs = (user_doc or {}).get("topic_prefs", {}) or {}

    for c in cands:
        overlap = _jaccard(topics, c.get("topics", []) or [])
        pref = user_prefs.get((c.get("topics") or [primary])[0], 0.0)
        fresh = _freshness_boost(c.get("published_at"))
        match = W_OVERLAP * overlap + W_PREF * pref + W_FRESH * fresh
        rs = W_SCORE * _normalize_score(float(c.get("score", 0.0)), lo, hi) + W_MATCH * match

        # light diversity rule: avoid immediate same series as the current one
        if c.get("podcast_id") == current_ep.get("podcast_id"):
            rs *= 0.98  # tiny penalty

        if rs > best_rs:
            best, best_rs = c, rs

    if not best:
        return None

    return {
        "episode_id": best["episode_id"],
        "topic": primary,
        "score": float(best_rs),
        "picked_at": _now_utc()
    }

# ========= FOREACH BATCH =========

def process_batch(batch_df, epoch_id: int):
    """
    Performs:
      - per-event metric updates (episodes + users)
      - episode score recompute
      - next_rec calculation on key events
    Uses bulkWrite for efficiency.
    """
    import pymongo
    from pymongo import UpdateOne

    if batch_df.rdd.isEmpty():
        return

    client = pymongo.MongoClient(MONGO_URL)
    eps = client[MONGO_DB][MONGO_EPISODES]
    users = client[MONGO_DB][MONGO_USERS]

    # Collect to driver for MVP; for higher throughput, switch to foreachPartition.
    rows = [r.asDict(recursive=True) for r in batch_df.collect()]

    # Preload episode docs for this batch to reduce roundtrips
    ep_ids = list({r["episode_id"] for r in rows if r.get("episode_id") is not None})
    ep_docs = {}
    for doc in eps.find({"episode_id": {"$in": ep_ids}},
                        {"episode_id": 1, "duration_sec": 1, "sentiment": 1, "stats": 1,
                         "topics": 1, "podcast_id": 1, "published_at": 1, "score": 1}):
        ep_docs[doc["episode_id"]] = doc

    # Bulk ops we’ll flush at the end (episodes + users)
    ep_ops: List[UpdateOne] = []
    user_ops: List[UpdateOne] = []

    for r in rows:
        uid = r["user_id"]
        eid = r["episode_id"]
        etype = r["event_type"]
        ts = r["ts"]
        rating = r.get("rating")
        position_sec = r.get("position_sec") or 0  # not in your current schema; safe default

        # Load episode doc (may be missing for rare races)
        e = ep_docs.get(eid) or eps.find_one({"episode_id": eid},
                                             {"episode_id": 1, "duration_sec": 1, "sentiment": 1, "stats": 1,
                                              "topics": 1, "podcast_id": 1, "published_at": 1, "score": 1})
        if not e:
            # if we don’t know the episode yet, skip safely
            continue

        dur = max(e.get("duration_sec", 1) or 1, 1)

        # ---- EPISODE METRICS ----
        inc = {}
        if etype == "play":
            inc["stats.plays"] = 1
        elif etype == "like":
            inc["stats.likes"] = 1
        elif etype == "complete":
            inc["stats.completions"] = 1
        elif etype == "skip":
            inc["stats.skips"] = 1
            if position_sec < 0.25 * dur:
                inc["stats.early_skips"] = 1
        elif etype == "rate" and rating is not None:
            inc["stats.rating_cnt"] = 1
            inc["stats.rating_sum"] = float(rating)

        if inc:
            ep_ops.append(UpdateOne(
                {"episode_id": eid},
                {"$inc": inc, "$set": {"updated_at": _now_utc()}},
                upsert=True
            ))

        # ---- RECOMPUTE EPISODE SCORE (read after inc; we’ll recompute once at the end) ----
        # For simplicity we’ll recompute using the doc we have + deltas aren’t applied yet.
        # To stay accurate, we’ll just ask Mongo again after bulk inc (second pass).
        # (See below: after bulk_write, we do a recompute pass.)

        # ---- USER METRICS & PREFS ----
        prefs_delta = 0.0
        if etype == "like":
            prefs_delta = 1.0
        elif etype == "complete":
            prefs_delta = 0.6
        elif etype == "play":
            prefs_delta = 0.1
        elif etype == "skip":
            prefs_delta = -0.8
        elif etype == "rate" and rating is not None:
            prefs_delta = (float(rating) - 3.0) * 0.4

        topics = e.get("topics", []) or []
        inc_prefs = {f"topic_prefs.{t}": prefs_delta for t in topics} if prefs_delta != 0 else {}

        set_fields = {"user_id": uid, "last_episode_id": eid, "last_event_at": _now_utc()}
        push_fields = {}
        add_to_set_fields = {"listened": {eid: True}}  # we’ll cap size later

        update_doc = {"$set": set_fields}
        if inc_prefs:
            update_doc["$inc"] = inc_prefs
        # maintain a small listened map using $set with dotted key, not $addToSet (array).
        for t in topics:
            pass  # (inc handled above)

        # Use $set to mark listened map key; Mongo needs dotted path updates one by one
        # So we construct a series of updates: we’ll just set listened.<eid> = true
        update_doc["$set"][f"listened.{eid}"] = True

        # Also maintain recent_recs cap via $push with $slice when we save a rec (see below)
        user_ops.append(UpdateOne(
            {"user_id": uid},
            update_doc,
            upsert=True
        ))

    # 1) Flush episode and user increments
    if ep_ops:
        eps.bulk_write(ep_ops, ordered=False)
    if user_ops:
        users.bulk_write(user_ops, ordered=False)

    # 2) Recompute scores for the involved episodes (accurate after increments)
    updated_ep_ids = list({r["episode_id"] for r in rows})
    recompute_ops: List[UpdateOne] = []
    for doc in eps.find({"episode_id": {"$in": updated_ep_ids}},
                        {"episode_id": 1, "stats": 1, "sentiment": 1}):
        new_score = _compute_episode_score(doc.get("stats", {}), float(doc.get("sentiment", 0.0) or 0.0))
        recompute_ops.append(UpdateOne(
            {"episode_id": doc["episode_id"]},
            {"$set": {"score": float(new_score), "updated_at": _now_utc()}}
        ))
    if recompute_ops:
        eps.bulk_write(recompute_ops, ordered=False)

    # 3) Compute next recommendations for key events only
    key_rows = [r for r in rows if r["event_type"] in ("play", "like", "complete", "rate")]
    for r in key_rows:
        uid, eid = r["user_id"], r["episode_id"]
        current_ep = eps.find_one({"episode_id": eid},
                                  {"episode_id": 1, "topics": 1, "podcast_id": 1, "published_at": 1})
        user_doc = users.find_one({"user_id": uid},
                                  {"user_id": 1, "topic_prefs": 1, "listened": 1, "recent_recs": 1})

        next_rec = _choose_next_rec(eps, user_doc, current_ep)
        if next_rec:
            # Update user with next_rec and keep recent_recs capped
            users.update_one(
                {"user_id": uid},
                {
                    "$set": {"next_rec": next_rec},
                    "$push": {"recent_recs": {"$each": [next_rec["episode_id"]], "$slice": -RECENT_RECS_CAP}}
                },
                upsert=True
            )

    client.close()

# ========= STREAM JOB =========

def main():
    spark = (SparkSession.builder
             .appName("user_events_stream")
             .config("spark.sql.shuffle.partitions", "64")
             .getOrCreate())

    # Kafka value schema (JSON)
    event_schema = T.StructType([
        T.StructField("event_id", T.LongType(), False),
        T.StructField("user_id", T.LongType(), False),
        T.StructField("episode_id", T.LongType(), False),
        T.StructField("ts", T.TimestampType(), False),
        T.StructField("event_type", T.StringType(), False),
        T.StructField("rating", T.DoubleType(), True),
        T.StructField("device", T.StringType(), True),
        # position_sec not in your current event; left here for potential future use:
        T.StructField("position_sec", T.IntegerType(), True),
    ])

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", KAFKA_TOPIC)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw
              .select(
                  F.col("timestamp").alias("kafka_ts"),
                  F.from_json(F.col("value").cast("string"), event_schema).alias("e"))
              .select(
                  "e.*",
                  F.col("kafka_ts").alias("ingested_at"))
              .withWatermark("ts", WATERMARK)
              .dropDuplicates(["event_id"]))  # de-dup within watermark

    # (Optional) basic sanity filter
    parsed = parsed.filter(
        (F.col("user_id").isNotNull()) &
        (F.col("episode_id").isNotNull()) &
        (F.col("event_type").isin("play", "pause", "like", "skip", "rate", "complete"))
    )

    # Write with foreachBatch
    query = (parsed.writeStream
             .outputMode("update")
             .foreachBatch(process_batch)
             .option("checkpointLocation", CHECKPOINT_PATH)
             .start())

    query.awaitTermination()

if __name__ == "__main__":
    main()
