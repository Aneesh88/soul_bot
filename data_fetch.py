# data_fetch.py
"""
Fetch new 1-min OHLCV bars via true_data_utils and insert them into the
SQLite 'bars' table, storing timestamps as "YYYY-MM-DD HH:MM:SS" strings.
"""

import logging
import pandas as pd
from trade_config import TradeConfig
from true_data_utils import fetch_latest_ohlcv
from db import get_conn

log = logging.getLogger(__name__)

def data_fetch_cycle() -> int:
    """
    1) Ensure DB & 'bars' table exist
    2) Read the last timestamp (ISO string) from 'bars'
    3) Fetch fresh bars via TrueData
    4) Normalize & filter to only those newer than last_ts
    5) Ensure symbol & date columns
    6) Convert timestamps to "YYYY-MM-DD HH:MM:SS"
    7) INSERT OR IGNORE into 'bars' table
    Returns number of rows inserted.
    """
    # 1) Init schema
    init_db()

    # 2) Get the last timestamp from bars (as "YYYY-MM-DD HH:MM:SS" or None)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT MAX(timestamp) FROM bars")
        row = cur.fetchone()
        last_ts = row[0] if row and row[0] is not None else None

    # 3) Fetch raw bars
    df = fetch_latest_ohlcv()
    if df is None or df.empty:
        log.debug("No new bars fetched.")
        return 0

    # 4) Parse & filter by last_ts
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])
    if last_ts:
        df = df[df['timestamp'] > pd.to_datetime(last_ts)]
    if df.empty:
        log.debug("No bars newer than last timestamp; nothing to insert.")
        return 0

    # 5) Ensure metadata
    if 'symbol' not in df.columns:
        df['symbol'] = TradeConfig.TD_SYMBOL
    if 'date' not in df.columns:
        df['date'] = df['timestamp'].dt.strftime("%Y-%m-%d")

    # 6) Format timestamps with a SPACE
    df['timestamp'] = df['timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")

    # 7) Bulk INSERT OR IGNORE
    cols = list(TradeConfig.BAR_COLS) + ['symbol', 'date']
    placeholders = ", ".join("?" for _ in cols)
    sql = f"INSERT OR IGNORE INTO bars ({','.join(cols)}) VALUES ({placeholders})"

    inserted = 0
    with get_conn() as conn:
        cur = conn.cursor()
        for vals in df[cols].itertuples(index=False, name=None):
            try:
                cur.execute(sql, vals)
                inserted += cur.rowcount
            except Exception as e:
                log.error("Failed to insert %s: %s", vals, e)
        conn.commit()

    log.info("Inserted %d new bars into SQLite 'bars' table", inserted)
    return inserted

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    count = data_fetch_cycle()
    print(f"✅ Inserted {count} new bars")
