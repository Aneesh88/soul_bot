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


def create_new_predictions_table():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS new_predictions (
        timestamp                      TIMESTAMP PRIMARY KEY,
        direction                      TEXT,
        confidence                     REAL,
        long_conf                      REAL,
        short_conf                     REAL,
        entry_smoothed_long_conf       REAL,
        entry_smoothed_short_conf      REAL,
        exit_smoothed_long_conf        REAL,
        exit_smoothed_short_conf       REAL
    );
    """

    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        print("✅ Table 'new_predictions' created successfully.")
    except Exception as e:
        print("❌ Error creating table:", e)

    

                # Live trade details (entry + exit records)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS live_trade_details (
    trade_number         INTEGER PRIMARY KEY,
    timestamp            TEXT NOT NULL,
    entry_time           TEXT,
    exit_time            TEXT,
    entry_order_id       TEXT,
    exit_order_id        TEXT,
    direction            TEXT NOT NULL,
    confidence           REAL,
    instrument           TEXT,
    quantity             INTEGER,
    entry_index_price    REAL,
    exit_index_price     REAL,
    index_pnl            REAL,
    entry_price_option   REAL,
    exit_price_option    REAL,
    option_pnl           REAL,
    exit_reason          TEXT,
    status               TEXT
       )
     """)


        # Daily trade state
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_trade_state (
            date                 TEXT PRIMARY KEY,
            last_trade_number    INTEGER,
            active_trade_count   INTEGER,
            closed_trade_count   INTEGER,
            win_count            INTEGER,
            loss_count           INTEGER,
            daily_pnl            REAL
        )
        """)

        


    conn.commit()

    from datetime import date



if __name__ == "__main__":
    init_db()
    print(f"✅ Initialized SQLite DB at {DB_PATH}")
