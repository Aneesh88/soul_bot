# dashboard.py
import streamlit as st
import pandas as pd
import sqlite3
from datetime import date, timedelta
from streamlit_autorefresh import st_autorefresh

DB_FILE = "your.db"  # adjust to your SQLite path
REFRESH_MS = 10 * 1000  # 10 seconds

st_autorefresh(interval=REFRESH_MS, key="dbrefresh")

def load_count_and_missing(tbl: str, target_date: date):
    """Return (count, list_of_missing) for a given table and date."""
    date_str = target_date.strftime("%Y-%m-%d")
    # load timestamps
    with sqlite3.connect(DB_FILE) as conn:
        df = pd.read_sql(
            f"SELECT timestamp FROM {tbl} WHERE date = ? ORDER BY timestamp",
            conn,
            params=(date_str,)
        )
    # count
    cnt = len(df)
    # build expected range 09:15–15:30
    idx = pd.date_range(
        start=f"{date_str} 09:15:00",
        end  =f"{date_str} 15:30:00",
        freq ="T"
    ).strftime("%Y-%m-%d %H:%M:%S")
    missing = sorted(set(idx) - set(df["timestamp"]))
    return cnt, missing

st.title("Live DB Monitor — Today vs. Yesterday")

today     = date.today()
yesterday = today - timedelta(days=1)

for tbl in ("bars", "features", "predictions"):
    col1, col2 = st.columns(2)
    # Today
    cnt_t, miss_t = load_count_and_missing(tbl, today)
    with col1:
        st.subheader(f"{tbl.capitalize()} — {today}")
        st.metric("Rows", cnt_t)
        if miss_t:
            st.warning(f"{len(miss_t)} missing (e.g. {miss_t[:3]})")
        else:
            st.success("No missing timestamps")
    # Yesterday
    cnt_y, miss_y = load_count_and_missing(tbl, yesterday)
    with col2:
        st.subheader(f"{tbl.capitalize()} — {yesterday}")
        st.metric("Rows", cnt_y)
        if miss_y:
            st.warning(f"{len(miss_y)} missing (e.g. {miss_y[:3]})")
        else:
            st.success("No missing timestamps")
