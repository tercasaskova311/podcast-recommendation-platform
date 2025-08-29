import os
import pandas as pd
import streamlit as st
from pymongo import MongoClient
import time
import duckdb

from config.settings import (
    MONGO_URI, MONGO_DB, 
    MONGO_COLLECTION_FINAL_RECS, 
    DELTA_EVENTS_PATH,
    DELTA_PATH_EPISODES 
)
st.set_page_config(page_title="Podcast Recommendation Platform – Simple", layout="wide")

#REFRESH ===========================
st.sidebar.header("Refresh")
if st.sidebar.button("Refresh now"):
    st.cache_data.clear()
    st.rerun()

auto = st.sidebar.checkbox("Auto-refresh")
every = st.sidebar.slider("Every (seconds)", 5, 60, 10, disabled=not auto) #set up timer to refresh the data
if auto:
    time.sleep(every)
    st.cache_data.clear()
    st.rerun()

#HELPERS ===============================
@st.cache_resource(show_spinner=False) #mongo client for streamlit seasion
def _client() -> MongoClient:
    return MongoClient(MONGO_URI, tz_aware=True)

@st.cache_data(ttl=5, show_spinner=False) #data is cached for 5 seconds - refresh....
def load_recs() -> pd.DataFrame: 
    col = _client()[MONGO_DB][MONGO_COLLECTION_FINAL_RECS]
    docs = list(col.find(
        {},
        {"_id": 0, "user_id": 1, "recommended_episode_id": 1, "score": 1, "generated_at": 1}
    ))
    df = pd.DataFrame(docs)
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
    return df

# ------ DuckDB - Delta ------
@st.cache_resource(show_spinner=False)
def _duck():
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    if DELTA_EVENTS_PATH.startswith("s3://"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("SET s3_region = $region", {"region": os.getenv("AWS_REGION", "eu-west-1")})
        con.execute("SET s3_access_key_id = $akid", {"akid": os.getenv("AWS_ACCESS_KEY_ID", "")})
        con.execute("SET s3_secret_access_key = $sak", {"sak": os.getenv("AWS_SECRET_ACCESS_KEY", "")})
        con.execute("SET s3_session_token = $tok", {"tok": os.getenv("AWS_SESSION_TOKEN", "")})
    return con

@st.cache_data(ttl=5, show_spinner=False)
def load_events_duck(minutes: int = 10) -> pd.DataFrame:
    """
    Reads only the last `minutes` from the Delta events table into a pandas DF.
    Expected columns: ts, user_id, episode_id, event, rating (rating optional).
    """
    try:
        con = _duck()
        df = con.execute(
            """
            WITH src AS (
              SELECT
                try_cast(ts AS TIMESTAMP) AS ts,  -- robust if ts is stored as string
                user_id, episode_id, event, rating
              FROM delta_scan(?)
            )
            SELECT *
            FROM src
            WHERE ts >= now() - (? * INTERVAL 1 MINUTE)
            ORDER BY ts ASC
            """,
            [DELTA_EVENTS_PATH, int(minutes)],
        ).df()
    except Exception as e:
        st.warning(f"Delta read failed: {e}")
        return pd.DataFrame(columns=["ts", "user_id", "episode_id", "event", "rating"])

    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    return df

@st.cache_data(ttl=60, show_spinner=False)
def load_episode_meta_duck() -> pd.DataFrame:  
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

win = st.sidebar.slider("Data from last X min", 5, 60, 10)
st.header(f"Data from last {win} min")
topk = st.sidebar.slider("Top K", 5, 100, 20)

w = {"like": 2.0, "complete": 3.0, "pause": 0.5, "rate": 1.0, "skip": -1.0}

#LOAD DATA ========================
events = load_events_duck(win)
recs = load_recs()
meta = load_episode_meta_duck() 

# attach episode_title to recs 
if not meta.empty:
    recs = recs.merge(meta, left_on="recommended_episode_id", right_on="episode_id", how="left")
    events = events.merge(meta, on="episode_id", how="left")

if "episode_title" in recs.columns:
    recs["episode_title"] = recs["episode_title"].fillna(recs["recommended_episode_id"].astype(str))
if "episode_title" in events.columns:
    events["episode_title"] = events["episode_title"].fillna(events["episode_id"].astype(str))


#ENGAGEMENT SCORE ================

def engagement_topk(events: pd.DataFrame, weights: dict, topk: int = 20) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=["episode_title", "engagement_score", "event_count"])
    ev = events.copy()
    ev["w"] = ev["event"].map(weights).fillna(0.0)
    agg = ev.groupby("episode_title")["w"].sum().rename("engagement_score").reset_index()
    cnt = ev.groupby("episode_title").size().rename("event_count").reset_index()
    out = agg.merge(cnt, on="episode_title", how="left")
    return out.sort_values(["engagement_score", "event_count"], ascending=[False, False]).head(topk)

#TOP RATED EPISODES =================
def top_rated_bayes(events: pd.DataFrame, m: int = 5, topk: int = 20) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=["episode_title", "avg_rating", "votes", "wr"])
    df = events.loc[events["event"].eq("rate"), ["episode_title", "rating"]].copy()
    if df.empty:
        return pd.DataFrame(columns=["episode_title", "avg_rating", "votes", "wr"])
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce").dropna()
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
    return g.sort_values(["wr", "votes"], ascending=[False, False]).head(topk)

# ---- UI ----------------------------
st.title("Podcast Recommendation – Platform")

# just some simple statistics
k1, k2, k3 = st.columns(3)
k1.metric("Users listening to podcasts", f"{recs['user_id'].nunique():,}" if 'user_id' in recs else "0")
k2.metric("Avg podcasts score", f"{recs['score'].mean():.3f}" if not recs.empty and 'score' in recs else "–")

if not events.empty and 'event' in events.columns and 'episode_title' in events.columns:
    liked_counts = events.loc[events['event'].eq('like'), 'episode_title'].value_counts()
    if len(liked_counts) > 0:
        top_title = liked_counts.index[0]
        likes = int(liked_counts.iloc[0])
        k3.metric(f"Most liked (last {win} min)", f"{top_title}", f"{likes} likes")
    else:
        k3.metric(f"Most liked (last {win} min)", "–")
else:
    k3.metric(f"Most liked (last {win} min)", "–")


# Sidebar filters
st.sidebar.header("Filters")
q = st.sidebar.text_input("user_id contains", "")
min_score = st.sidebar.number_input("Min podcast score", value=0.0, step=0.1)
limit_rows = st.sidebar.slider("Max rows in the dashboard", 50, 5000, 500)

# Apply filters
view = recs.copy()
if q:
    view = view[view["user_id"].astype(str).str.contains(q, case=False, na=False)]
if "score" in view.columns:
    view = view[view["score"].fillna(-1) >= min_score]

#DASHBOARD ======================
cols_to_show = [c for c in ["user_id", "episode_title", "score", "generated_at"] if c in view.columns]
st.markdown("### Personalized recommendations")
st.caption("Generated from recent user behavior + podcast similarity.")
st.dataframe(view[cols_to_show].head(limit_rows))
st.caption("Recommended podcasts based on user behavior + podcasts similarity.")


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

# Top engagement
st.markdown("#### Top Episodes by User Engagement")
eng = engagement_topk(events, w, topk)
st.dataframe(eng if not eng.empty else pd.DataFrame(columns=["episode_title", "engagement_score", "event_count"]))

# Top rated (optional)
st.markdown("#### Top Rated Podcasts Episodes")
rated = top_rated_bayes(events, m=5, topk=topk)
if not rated.empty:
    st.dataframe(rated)
else:
    st.caption("Recently there aren't rated events.")
