# migrate_all_to_db.py
"""
One‐time migration of your three CSVs into the SQLite database,
normalizing all column names to snake_case lower-case (without leading underscores),
and replacing any existing tables so you get a clean slate.
"""

import pandas as pd
from pathlib import Path
from db import init_db, get_conn
from trade_config import TradeConfig

def normalize_columns(cols: list[str]) -> list[str]:
    """
    Given a list of column names, return a new list where each name is:
      - stripped of leading/trailing whitespace
      - camelCase converted to snake_case (no leading underscore)
      - lower-cased
      - spaces replaced with underscores
      - multiple underscores collapsed to single
    """
    normalized = []
    for col in cols:
        c = col.strip()
        new = []
        for idx, ch in enumerate(c):
            if ch.isupper():
                # only prefix underscore if not the first character
                if idx != 0:
                    new.append("_")
                new.append(ch.lower())
            else:
                new.append(ch)
        c2 = "".join(new)
        c2 = c2.replace(" ", "_").lower()
        # collapse any double underscores
        while "__" in c2:
            c2 = c2.replace("__", "_")
        normalized.append(c2)
    return normalized

def migrate_bars(conn):
    csv_path = Path(TradeConfig.CORE_CSV)
    print(f"Loading bars from {csv_path} …")
    df = pd.read_csv(
        csv_path,
        parse_dates=["timestamp"],
        dtype={"open": float, "high": float, "low": float, "close": float,
               "volume": int, "oi": int, "symbol": str, "date": str}
    )
    # normalize header names
    df.columns = normalize_columns(list(df.columns))
    # rename legacy 'oi' → 'open_interest'
    if "oi" in df.columns and "open_interest" not in df.columns:
        df.rename(columns={"oi": "open_interest"}, inplace=True)

    # drop & recreate the table
    conn.execute("DROP TABLE IF EXISTS bars")
    df.to_sql("bars", conn, if_exists="replace", index=False)
    print(f"  → Inserted {len(df)} rows into bars")

def migrate_features(conn):
    csv_path = Path(TradeConfig.FEATURES_CSV)
    print(f"Loading features from {csv_path} …")
    df = pd.read_csv(
        csv_path,
        parse_dates=["timestamp", "datetime"],
        low_memory=False
    )
    # normalize header names
    df.columns = normalize_columns(list(df.columns))
    # ensure boolean dtype for ema_filter_15
    if "ema_filter_15" in df.columns:
        df["ema_filter_15"] = df["ema_filter_15"].astype(bool)

    # drop & recreate the table
    conn.execute("DROP TABLE IF EXISTS features")
    df.to_sql("features", conn, if_exists="replace", index=False)
    print(f"  → Inserted {len(df)} rows into features")

def migrate_predictions(conn):
    csv_path = Path(TradeConfig.PRED_CSV)
    print(f"Loading predictions from {csv_path} …")
    if not csv_path.exists():
        print("  → predictions CSV not found; skipping.")
        return

    df = pd.read_csv(csv_path)
    # normalize header names
    df.columns = normalize_columns(list(df.columns))
    # convert timestamp if present
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    else:
        print("  → 'timestamp' column not in predictions CSV; skipping.")
        return

    # drop & recreate the table
    conn.execute("DROP TABLE IF EXISTS predictions")
    df.to_sql("predictions", conn, if_exists="replace", index=False)
    print(f"  → Inserted {len(df)} rows into predictions")

def main():
    # 1) Ensure DB and (stub) tables exist
    init_db()

    # 2) Migrate each CSV into its SQLite table, replacing any existing data
    with get_conn() as conn:
        migrate_bars(conn)
        migrate_features(conn)
        migrate_predictions(conn)

    print("✅ Migration complete into core_files/trading_data.db")

if __name__ == "__main__":
    main()
