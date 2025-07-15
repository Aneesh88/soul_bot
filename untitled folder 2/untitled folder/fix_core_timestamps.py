# fix_core_timestamps.py
"""
Backfill any NaT entries in the 'timestamp' column of your core CSV
by taking the previous non-null timestamp and adding one minute.
"""

import pandas as pd
from pathlib import Path

CORE_CSV = Path("/Users/aneeshviswanathan/Desktop/om_babaji_om/core_files/JAN2024_TO_JUN2025_BANKNIFTY_FUT.csv")

def main():
    # 1) Load, parse timestamp
    df = pd.read_csv(CORE_CSV)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    # 2) Iterate and backfill minute by minute
    last_valid = None
    for idx in df.index:
        ts = df.at[idx, 'timestamp']
        if pd.isna(ts):
            if last_valid is None:
                raise ValueError(f"Row {idx} has no prior valid timestamp to fill from")
            df.at[idx, 'timestamp'] = last_valid + pd.Timedelta(minutes=1)
        last_valid = df.at[idx, 'timestamp']

    # 3) Save back
    df.to_csv(CORE_CSV, index=False)
    print(f"✅ Filled missing timestamps; saved {CORE_CSV}")

if __name__ == "__main__":
    main()
