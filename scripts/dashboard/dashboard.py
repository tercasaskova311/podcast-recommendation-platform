import os
import pandas as pd
import streamlit as st
from pymongo import MongoClient
import time
import duckdb

from config.settings import (
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION_FINAL_RECS,
    MONGO_COLLECTION_USERS,
    DELTA_RAW_USER_EVENTS_PATH,   # raw events written by streaming
    DELTA_PATH_EPISODES,          # episode_id -> episode_title
)

st.set_page_config(page_title="Podcast Recommendation Platform – Simple", layout="wide")

# ========================= REFRESH =========================
st.sidebar.header("Refresh")
if st.sidebar.button("Refresh now"):
    st.cache_data.clear()
    st.rerun()

auto = st.sidebar.checkbox("Auto-refresh")
every = st.sidebar.slider("Every (seconds)", 5, 60, 10, disabled=not auto)
if auto:
    time.sleep(every)
    st.cache_data.clear()
    st.rerun()

# ========================= HELPERS =========================
@st.cache_resource(show_spinner=False)
def _client() -> MongoClient:
    return MongoClient(MONGO_URI, tz_aware=True)

@st.cache_data(ttl=5, show_spinner=False)
def load_recs() -> pd.DataFrame:
    col = _client()[MONGO_DB][MONGO_COLLECTION_FINAL_RECS]
    docs = list(
        col.find(
            {},
            {"_id": 0, "user_id": 1, "recommended_episode_id": 1, "score": 1, "generated_at": 1},
        )
    )
    df = pd.DataFrame(docs)
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
    if not df.empty:
        df["user_id"] = df["user_id"].astype(str)
        df["recommended_episode_id"] = df["recommended_episode_id"].astype(str)
    return df

@st.cache_data(ttl=60, show_spinner=False)
def load_users() -> pd.DataFrame:
    col = _client()[MONGO_DB][MONGO_COLLECTION_USERS]
    docs = list(
        col.find({}, {"_id": 0, "id": 1, "name": 1, "surname": 1, "date_of_birth": 1, "gender": 1})
    )
    df = pd.DataFrame(docs)
    if not df.empty:
        df.rename(columns={"id": "user_id"}, inplace=True)
        df["user_id"] = df["user_id"].astype(str)
        for c in ["name", "surname", "gender", "date_of_birth"]:
            if c in df.columns:
                df[c] = df[c].astype(str)
    return df

# ------ DuckDB - Delta ------
@st.cache_resource(show_spinner=False)
def _duck():
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    if any(p.startswith("s3://") for p in [DELTA_RAW_USER_EVENTS_PATH, DELTA_PATH_EPISODES]):
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("SET s3_region = $region", {"region": os.getenv("AWS_REGION", "eu-west-1")})
        con.execute("SET s3_access_key_id = $akid", {"akid": os.getenv("AWS_ACCESS_KEY_ID", "")})
        con.execute("SET s3_secret_access_key = $sak", {"sak": os.getenv("AWS_SECRET_ACCESS_KEY", "")})
        con.execute("SET s3_session_token = $tok", {"tok": os.getenv("AWS_SESSION_TOKEN", "")})
    return con

@st.cache_data(ttl=5, show_spinner=False)
def load_events_duck(minutes: int = 10) -> pd.DataFrame:
    """
    Reads only the last `minutes` of raw user events from Delta.
    Expected columns: ts, user_id, episode_id, event, rating.
    """
    try:
        con = _duck()
        df = con.execute(
            """
            WITH src AS (
              SELECT
                try_cast(ts AS TIMESTAMP) AS ts,
                CAST(user_id AS VARCHAR) AS user_id,
                CAST(episode_id AS VARCHAR) AS episode_id,
                event, rating
              FROM delta_scan(?)
            )
            SELECT *
            FROM src
            WHERE ts >= now() - (? * INTERVAL 1 MINUTE)
            ORDER BY ts ASC
            """,
            [DELTA_RAW_USER_EVENTS_PATH, int(minutes)],
        ).df()
    except Exception as e:
        st.warning(f"Delta read failed: {e}")
        return pd.DataFrame(columns=["ts", "user_id", "episode_id", "event", "rating"])

    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        df["user_id"] = df["user_id"].astype(str)
        df["episode_id"] = df["episode_id"].astype(str)
    return df

@st.cache_data(ttl=60, show_spinner=False)
def load_episode_meta_duck() -> pd.DataFrame:
    """
    Episode metadata with titles. Must contain: episode_id, episode_title.
    """
    try:
        con = _duck()
        df = con.execute(
            """
            SELECT DISTINCT
                CAST(episode_id AS VARCHAR) AS episode_id,
                episode_title
            FROM delta_scan(?)
            """,
            [DELTA_PATH_EPISODES],
        ).df()
    except Exception as e:
        st.warning(f"Episodes meta read failed: {e}")
        return pd.DataFrame(columns=["episode_id", "episode_title"])

    if not df.empty:
        df["episode_id"] = df["episode_id"].astype(str)
    return df

# ========================= UI CONTROLS =========================
win = st.sidebar.slider("Data from last X min", 5, 60, 10)
st.header(f"Data from last {win} min")
topk = st.sidebar.slider("Top K", 5, 100, 20)

w = {"like": 2.0, "complete": 3.0, "pause": 0.5, "rate": 1.0, "skip": -1.0}

# ========================= LOAD DATA =========================
events = load_events_duck(win)
recs = load_recs()
meta = load_episode_meta_duck()
users = load_users()

# attach episode_title to recs & events
if not meta.empty:
    if not recs.empty:
        recs = recs.merge(meta, left_on="recommended_episode_id", right_on="episode_id", how="left")
    if not events.empty:
        events = events.merge(meta, on="episode_id", how="left")

if "episode_title" in recs.columns:
    recs["episode_title"] = recs["episode_title"].fillna(recs["recommended_episode_id"].astype(str))
if "episode_title" in events.columns:
    events["episode_title"] = events["episode_title"].fillna(events["episode_id"].astype(str))

# ========================= METRICS =========================
st.title("Podcast Recommendation – Platform")

k1, k2, k3 = st.columns(3)
k1.metric("Users with recommendations", f"{recs['user_id'].nunique():,}" if "user_id" in recs.columns else "0")
k2.metric("Avg recommendation score", f"{recs['score'].mean():.3f}" if not recs.empty and "score" in recs.columns else "–")

if not events.empty and "event" in events.columns and "episode_title" in events.columns:
    liked_counts = events.loc[events["event"].eq("like"), "episode_title"].value_counts()
    if len(liked_counts) > 0:
        top_title = liked_counts.index[0]
        likes = int(liked_counts.iloc[0])
        k3.metric(f"Most liked (last {win} min)", f"{top_title}", f"{likes} likes")
    else:
        k3.metric(f"Most liked (last {win} min)", "–")
else:
    k3.metric(f"Most liked (last {win} min)", "–")

# ========================= FIRST TABLE =========================
# Users + their top recommended episode (top-1 per user by score).
# Keep only the top 20 rows overall by score, then order by lastname(surname) and name.
# IMPORTANT: Exclude rows without a matching user profile (inner join).
st.markdown("### Users and Top Recommended Episode")
st.caption("Columns: name, lastname, date of birth, gender, episode title suggested. Only users with a profile are shown. Top 20 by score, ordered by lastname and name.")

top_table = pd.DataFrame(columns=["name", "lastname", "date_of_birth", "gender", "episode_title"])

if not recs.empty:
    r = recs.copy()
    for c in ["user_id", "episode_title", "score"]:
        if c not in r.columns:
            r[c] = None
    r["score"] = pd.to_numeric(r["score"], errors="coerce")

    # choose per-user top1 by score
    r = r.sort_values(["user_id", "score"], ascending=[True, False])
    per_user_top1 = r.dropna(subset=["user_id"]).drop_duplicates(subset=["user_id"], keep="first")

    # INNER join => drop rows without a profile
    if not users.empty:
        per_user_top1 = per_user_top1.merge(users, on="user_id", how="inner")
        if not per_user_top1.empty:
            per_user_top1 = per_user_top1.dropna(subset=["name", "surname"])
            per_user_top1 = per_user_top1[
                (per_user_top1["name"].astype(str).str.strip() != "") &
                (per_user_top1["surname"].astype(str).str.strip() != "")
            ]
    else:
        per_user_top1 = per_user_top1.iloc[0:0]

    if not per_user_top1.empty:
        tbl = per_user_top1[["name", "surname", "date_of_birth", "gender", "episode_title", "score"]].copy()
        tbl.rename(columns={"surname": "lastname"}, inplace=True)
        tbl = tbl.sort_values("score", ascending=False).head(20)   # top 20 by score
        for c in ["lastname", "name", "date_of_birth", "gender", "episode_title"]:
            if c not in tbl.columns:
                tbl[c] = ""
        tbl = tbl.sort_values(["lastname", "name"], ascending=[True, True])
        top_table = tbl[["name", "lastname", "date_of_birth", "gender", "episode_title"]]

st.dataframe(top_table, use_container_width=True)

# ========================= LIVE ENGAGEMENT =========================
st.markdown("---")
st.header(f"Live engagement – last {win} min")

st.markdown("#### Minute-by-minute activity")
if not events.empty:
    hb = events.copy()
    hb["ts"] = pd.to_datetime(hb["ts"], utc=True, errors="coerce")
    hb["minute"] = hb["ts"].dt.floor("min")
    epm = hb.groupby("minute").size().rename("events_per_min").reset_index()
    st.line_chart(epm.set_index("minute"))
else:
    st.info("No events in this window.")

def engagement_topk(events_df: pd.DataFrame, weights: dict, topk_n: int = 20) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame(columns=["episode_title", "engagement_score", "event_count"])
    ev = events_df.copy()
    ev["w"] = ev["event"].map(weights).fillna(0.0)
    agg = ev.groupby("episode_title")["w"].sum().rename("engagement_score").reset_index()
    cnt = ev.groupby("episode_title").size().rename("event_count").reset_index()
    out = agg.merge(cnt, on="episode_title", how="left")
    return out.sort_values(["engagement_score", "event_count"], ascending=[False, False]).head(topk_n)

st.markdown("#### Top Episodes by User Engagement")
eng = engagement_topk(events, w, topk)
st.dataframe(eng if not eng.empty else pd.DataFrame(columns=["episode_title", "engagement_score", "event_count"]), use_container_width=True)

def top_rated_bayes(events_df: pd.DataFrame, m: int = 5, topk_n: int = 20) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame(columns=["episode_title", "avg_rating", "votes", "wr"])
    df = events_df.loc[events_df["event"].eq("rate"), ["episode_title", "rating"]].copy()
    if df.empty:
        return pd.DataFrame(columns=["episode_title", "avg_rating", "votes", "wr"])
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df = df.dropna(subset=["rating"])
    if df.empty:
        return pd.DataFrame(columns=["episode_title", "avg_rating", "votes", "wr"])
    C = df["rating"].mean()
    g = (
        df.groupby("episode_title")
        .agg(avg_rating=("rating", "mean"), votes=("rating", "count"))
        .reset_index()
    )
    v, R = g["votes"], g["avg_rating"]
    g["wr"] = (v / (v + m)) * R + (m / (v + m)) * C
    return g.sort_values(["wr", "votes"], ascending=[False, False]).head(topk_n)

st.markdown("#### Top Rated Podcast Episodes")
rated = top_rated_bayes(events, m=5, topk_n=topk)
if not rated.empty:
    st.dataframe(rated, use_container_width=True)
else:
    st.caption("Recently there aren't rated events.")
