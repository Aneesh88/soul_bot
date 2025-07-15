#!/usr/bin/env python3
import datetime
import sqlite3
import pandas as pd
from db import init_db, get_conn

# ────── USER CONFIG ────────
# Set the date you want to check here (YYYY, M, D)
TARGET_DATE = datetime.date(2025, 6, 19)
# ───────────────────────────

def normalize_ts(ts):
    """
    Ensures timestamp is a string in 'YYYY-MM-DD HH:MM:SS' format.
    """
    if isinstance(ts, (datetime.datetime, datetime.date)):
        if isinstance(ts, datetime.date) and not isinstance(ts, datetime.datetime):
            ts = datetime.datetime.combine(ts, datetime.time.min)
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    try:
        import pandas as _pd
        if isinstance(ts, _pd.Timestamp):
            return ts.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")
    except ImportError:
        pass
    s = str(ts)
    if len(s) > 19:
        s = s[:19]
    return s


def get_timestamps(conn, table, start_str, end_str):
    cur = conn.cursor()
    cur.execute(f"""
        SELECT timestamp
          FROM {table}
         WHERE timestamp BETWEEN ? AND ?
         ORDER BY timestamp
    """, (start_str, end_str))
    return [normalize_ts(row[0]) for row in cur.fetchall()]


def main(target_date: datetime.date):
    start_str = f"{target_date} 09:15:00"
    end_str   = f"{target_date} 15:30:00"

    init_db()
    expected_times = pd.date_range(start=start_str, end=end_str, freq="T")
    expected = [ts.strftime("%Y-%m-%d %H:%M:%S") for ts in expected_times]

    with get_conn() as conn:
        for table in ("bars", "features", "predictions"):
            actual = get_timestamps(conn, table, start_str, end_str)
            duplicates = sorted({t for t in set(actual) if actual.count(t) > 1})
            missing = sorted(set(expected) - set(actual))
            unexpected = sorted(set(actual) - set(expected))

            print(f"\n=== Table `{table}` on {target_date} ===")
            print(f"Expected rows: {len(expected)}")
            print(f"Actual   rows: {len(actual)}")
            print(f"First timestamp: {actual[0] if actual else 'N/A'}")
            print(f"Last  timestamp: {actual[-1] if actual else 'N/A'}")

            if duplicates:
                print(f"  ⚠️ {len(duplicates)} duplicate timestamps:")
                for t in duplicates[:5]: print(f"    {t}")
                if len(duplicates) > 5: print(f"    ... and {len(duplicates)-5} more")
            if missing:
                print(f"  ❌ Missing {len(missing)} timestamps (up to 5):")
                for m in missing[:5]: print(f"    {m}")
                if len(missing) > 5: print(f"    ... and {len(missing)-5} more")
            if unexpected:
                print(f"  ⚠️ {len(unexpected)} unexpected timestamps (up to 5):")
                for u in unexpected[:5]: print(f"    {u}")
                if len(unexpected) > 5: print(f"    ... and {len(unexpected)-5} more")

            if not (duplicates or missing or unexpected):
                print(f"  ✅ All {table} timestamps are continuous and correct.")

if __name__ == "__main__":
    main(TARGET_DATE)
