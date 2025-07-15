#!/usr/bin/env python3
"""
backfill_offline_fp.py

Resets, backfills, and auto-fills missing bars, features, and predictions
for a given date from CSVs in the same folder, then runs a per-minute
integrity check for the 09:15–15:30 trading session.
"""

import sys
import warnings
from pathlib import Path
from collections import Counter
from datetime import datetime

import pandas as pd

from db import init_db, get_conn

# Suppress known deprecation warnings
warnings.filterwarnings('ignore', message=".*'T' is deprecated and will be removed.*")
warnings.filterwarnings('ignore', message=".*Downcasting object dtype arrays on .* is deprecated.*")
warnings.filterwarnings('ignore', category=DeprecationWarning)

# ─── 0) CONFIGURATION ──────────────────────────────────────────────────────────
# Set the date to process here (ISO YYYY-MM-DD):
DATE_STR = "2025-06-19"

# Use the script's directory as the CSV folder:
CSV_DIR = Path(__file__).resolve().parent

# CSV filenames:
BARS_CSV        = CSV_DIR / f"bars_{DATE_STR}.csv"
FEATURES_CSV    = CSV_DIR / f"features_{DATE_STR}.csv"
PREDICTIONS_CSV = CSV_DIR / f"predictions_{DATE_STR}.csv"

# Expected columns for bars table:
EXPECTED_BARS_COLS = [
    "timestamp", "open", "high", "low", "close", "volume", "open_interest"
]

# Trading session range:
SESSION_START = f"{DATE_STR} 09:15:00"
SESSION_END   = f"{DATE_STR} 15:30:00"

# ─── HELPERS ───────────────────────────────────────────────────────────────────

def load_csv(path: Path) -> pd.DataFrame:
    """Load CSV and normalize timestamp to string format."""
    if not path.exists():
        print(f"❌ ERROR: CSV not found: {path}", file=sys.stderr)
        sys.exit(1)
    df = pd.read_csv(path)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def generate_expected_index() -> pd.DatetimeIndex:
    """Generate a full minute index for the trading session."""
    return pd.date_range(start=SESSION_START, end=SESSION_END, freq="min")


def fill_bars(df: pd.DataFrame) -> pd.DataFrame:
    """Reindex bars to full session and forward-fill or zero-fill missing rows."""
    idx = generate_expected_index()
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp").reindex(idx)
    # forward-fill price and open_interest
    for col in ["open", "high", "low", "close", "open_interest"]:
        df[col] = df[col].ffill()
    # reset volume for filled rows
    df["volume"] = df["volume"].fillna(0)
    df = df.reset_index().rename(columns={"index": "timestamp"})
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def fill_generic(df: pd.DataFrame) -> pd.DataFrame:
    """Reindex generic table (features/predictions) and forward-fill missing rows."""
    idx = generate_expected_index()
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp").reindex(idx)
    df = df.ffill()
    df = df.infer_objects(copy=False)
    df = df.reset_index().rename(columns={"index": "timestamp"})
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def clear_date_rows(table: str):
    """Delete all rows in `table` for the given date to avoid duplication."""
    like_pattern = f"{DATE_STR}%"
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table} WHERE timestamp LIKE ?", (like_pattern,))
        deleted = cur.rowcount
        conn.commit()
    print(f"🗑️  Cleared {deleted} rows from {table} for {DATE_STR}")


def bulk_backfill(table: str, df: pd.DataFrame) -> int:
    """Insert all rows from df into `table`. Returns number of rows inserted."""
    init_db()
    cols = list(df.columns)
    placeholders = ", ".join("?" for _ in cols)
    col_list     = ", ".join(cols)
    rows = [tuple(r) for r in df[cols].to_numpy()]
    with get_conn() as conn:
        cur = conn.cursor()
        cur.executemany(
            f"INSERT OR IGNORE INTO {table} ({col_list}) VALUES ({placeholders})", rows
        )
        inserted = cur.rowcount
        conn.commit()
    print(f"🔎 {table:<11} → inserted {inserted} rows")
    return inserted


def integrity_check(table: str) -> dict:
    """Check table timestamps against expected session grid."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT timestamp FROM {table} WHERE timestamp BETWEEN ? AND ?", (SESSION_START, SESSION_END)
        )
        raw = [r[0] for r in cur.fetchall()]
    actual = []
    for ts in raw:
        if isinstance(ts, datetime):
            actual.append(ts.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            actual.append(str(ts)[:19])
    actual.sort()
    expected = generate_expected_index().strftime("%Y-%m-%d %H:%M:%S").tolist()
    dup        = [t for t, c in Counter(actual).items() if c > 1]
    missing    = sorted(set(expected) - set(actual))
    unexpected = sorted(set(actual) - set(expected))
    return {"table": table, "expected": len(expected), "actual": len(actual),
            "duplicates": dup, "missing": missing, "unexpected": unexpected}


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"▶️ Processing DATE {DATE_STR!r} in CSV_DIR {CSV_DIR}\n")

    # 1) Load raw CSVs
    raw_bars  = load_csv(BARS_CSV)
    raw_feats = load_csv(FEATURES_CSV)
    raw_preds = load_csv(PREDICTIONS_CSV)

    # 2) Normalize bars schema and auto-fill missing
    if "oi" in raw_bars.columns:
        raw_bars = raw_bars.rename(columns={"oi": "open_interest"})
    bars = raw_bars[[c for c in raw_bars.columns if c in EXPECTED_BARS_COLS]]
    bars = fill_bars(bars)

    # 3) Auto-fill features & predictions gaps
    feats = fill_generic(raw_feats)
    preds = fill_generic(raw_preds)

    # 4) Clear existing date rows
    for tbl in ("bars", "features", "predictions"):
        clear_date_rows(tbl)

    # 5) Backfill into database
    n_b = bulk_backfill("bars", bars)
    n_f = bulk_backfill("features", feats)
    n_p = bulk_backfill("predictions", preds)
    print(f"\n✅ Backfilled → bars: {n_b}, features: {n_f}, predictions: {n_p}\n")

    # 6) Integrity report
    print("── Integrity Check (09:15–15:30) ────────────────────────────────────────")
    for tbl in ("bars", "features", "predictions"):
        res = integrity_check(tbl)
        print(f"\n• {tbl:<11} expected {res['expected']}, found {res['actual']}")
        if res['duplicates']:
            print(f"  ⚠️ {len(res['duplicates'])} duplicates (e.g. {res['duplicates'][:3]}…)")
        if res['missing']:
            print(f"  ❌ {len(res['missing'])} missing (e.g. {res['missing'][:3]}…)")
        if res['unexpected']:
            print(f"  ❗ {len(res['unexpected'])} unexpected (e.g. {res['unexpected'][:3]}…)")
        if not (res['duplicates'] or res['missing'] or res['unexpected']):
            print("  ✅ No issues")

    print("\n🎉 All done.")