"""
Shows: Latest recommendations per user (reading Mongo docs that your batch overwrites)
Run: streamlit run app.py
"""
import os
import pandas as pd
import streamlit as st
from pymongo import MongoClient
import time
from config.settings import MONGO_URI, MONGO_DB, MONGO_COLLECTION_FINAL_RECS

st.set_page_config(page_title="Final Recs – Simple", layout="wide")

st.sidebar.header("Refresh")
if st.sidebar.button("Refresh now"):
    st.cache_data.clear()
    st.rerun()

auto = st.sidebar.checkbox("Auto-refresh")
every = st.sidebar.slider("Every (seconds)", 5, 60, 10, disabled=not auto)
if auto:
    # simple timer; clears cache and re-runs
    time.sleep(every)
    st.cache_data.clear()
    st.rerun()

# ---- DB helpers ----
@st.cache_resource(show_spinner=False)
def _client() -> MongoClient:
    return MongoClient(MONGO_URI, tz_aware=True)

@st.cache_data(ttl=5, show_spinner=False)
def load_recs() -> pd.DataFrame:
    col = _client()[MONGO_DB][MONGO_COLLECTION_FINAL_RECS]
    docs = list(col.find({}, {"_id": 0}))
    if not docs:
        return pd.DataFrame(columns=["user_id", "recommended_episode_id", "score", "generated_at"]) 
    df = pd.DataFrame(docs)
    # normalize types lightly; we won't compute on generated_at, only display
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
    return df

# ---- UI ----
st.title("Podcast Recommendations – Dashboard")

recs = load_recs()


# Sidebar filters
st.sidebar.header("Filters")
q = st.sidebar.text_input("user_id contains", "")
min_score = st.sidebar.number_input("Min score", value=0.0, step=0.1)
limit_rows = st.sidebar.slider("Max rows", 50, 5000, 500)

# Apply filters
view = recs.copy()
if q:
    view = view[view["user_id"].astype(str).str.contains(q, case=False, na=False)]
if "score" in view.columns:
    view = view[view["score"].fillna(-1) >= min_score]

# Show table with only the essentials
cols_to_show = [c for c in ["user_id", "recommended_episode_id", "score", "generated_at"] if c in view.columns]

st.markdown("### Recommendations")
st.dataframe(view[cols_to_show].head(limit_rows))

st.caption("Shows the latest user → recommendation pairs written by your batch job.")
