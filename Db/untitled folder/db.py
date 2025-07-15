# db.py
"""
Simple SQLite helper for storing raw bars, features, and predictions.
"""

import sqlite3
from pathlib import Path

# 1) Define the path to your DB file
DB_PATH = Path(__file__).resolve().parent / "core_files" / "trading_data.db"
DB_PATH.parent.mkdir(exist_ok=True)

def get_conn():
    """
    Returns a sqlite3.Connection to the database.
    """
    return sqlite3.connect(str(DB_PATH), detect_types=sqlite3.PARSE_DECLTYPES)

def init_db():
    """
    Creates the tables if they don’t exist yet.
    """
    with get_conn() as conn:
        cur = conn.cursor()

        # Raw bars table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bars (
            timestamp        TIMESTAMP PRIMARY KEY,
            open             REAL,
            high             REAL,
            low              REAL,
            close            REAL,
            volume           INTEGER,
            open_interest    INTEGER,
            symbol           TEXT,
            date             DATE
        )
        """)

        # Features table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS features (
            timestamp        TIMESTAMP PRIMARY KEY,
            ema_filter_15    BOOLEAN,
            open             REAL,
            high             REAL,
            low              REAL,
            close            REAL,
            volume           INTEGER,
            open_interest    INTEGER,
            -- add here all your other feature columns
            -- e.g. atr REAL, body_size REAL, … symbol TEXT
            symbol           TEXT
        )
        """)

        # Predictions table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            timestamp   TIMESTAMP PRIMARY KEY,
            direction   TEXT,
            confidence  REAL
        )
        """)

        conn.commit()

if __name__ == "__main__":
    init_db()
    print(f"✅ Initialized SQLite DB at {DB_PATH}")
