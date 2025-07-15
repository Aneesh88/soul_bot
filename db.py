# db.py
# Utility functions for DB interaction (SQLite3)

import sqlite3
import pandas as pd
from contextlib import contextmanager
from trade_config import BASE_DIR

DB_PATH = BASE_DIR / "core_files" / "trading_data.db"


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()


def read_table(table_name: str) -> pd.DataFrame:
    """Generic fetch for full table."""
    with get_conn() as conn:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn, parse_dates=["timestamp"])
    return df


def read_today_predictions() -> pd.DataFrame:
    """Fetch today's predictions (new_predictions table)."""
    sql = """
        SELECT *
          FROM new_predictions
         WHERE date(timestamp) = date('now', 'localtime')
         ORDER BY timestamp
    """
    with get_conn() as conn:
        df = pd.read_sql(sql, conn, parse_dates=["timestamp"])
    return df


def get_active_trade_count() -> int:
    """Return how many trades are currently open."""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) 
              FROM live_trade_details
             WHERE status = 'OPEN'
        """)
        return cursor.fetchone()[0]


def get_last_trade_number() -> int:
    """Return last trade number used today."""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT last_trade_number
              FROM daily_trade_state
             WHERE date = date('now', 'localtime')
        """)
        row = cursor.fetchone()
        return row[0] if row else 0


def increment_trade_number_and_update_state():
    """Increments today's trade number and updates state."""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO daily_trade_state (date, last_trade_number, active_trade_count, closed_trade_count, win_count, loss_count, daily_pnl)
            VALUES (date('now', 'localtime'), 1, 0, 0, 0, 0, 0.0)
            ON CONFLICT(date)
            DO UPDATE SET last_trade_number = last_trade_number + 1
        """)
        conn.commit()


def insert_live_trade(trade: dict):
    """Insert a new trade into live_trade_details."""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO live_trade_details (
                trade_number, timestamp, entry_time, entry_order_id, direction, confidence,
                instrument, quantity, entry_index_price, entry_price_option, status,
                fut_index_sl_level, fut_index_tp_level, strike, raw_confidence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade['trade_number'], trade['timestamp'], trade['entry_time'], trade['entry_order_id'], trade['direction'],
            trade['confidence'], trade['instrument'], trade['quantity'], trade['entry_index_price'],
            trade['entry_price_option'], trade['status'], trade['fut_index_sl_level'], trade['fut_index_tp_level'],
            trade['strike'], trade.get('raw_confidence')
        ))
        conn.commit()


def update_trade_exit(trade_number: int, updates: dict):
    """Update exit-related fields in live_trade_details."""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE live_trade_details
               SET exit_time = ?, exit_order_id = ?, exit_index_price = ?, exit_price_option = ?,
                   option_pnl = ?, index_pnl = ?, exit_reason = ?, status = ?
             WHERE trade_number = ?
        """, (
            updates['exit_time'], updates['exit_order_id'], updates['exit_index_price'],
            updates['exit_price_option'], updates['option_pnl'], updates['index_pnl'],
            updates['exit_reason'], updates['status'], trade_number
        ))
        conn.commit()


def update_daily_pnl(win: bool, pnl: float):
    """Update daily P&L and trade stats."""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE daily_trade_state
               SET closed_trade_count = closed_trade_count + 1,
                   win_count = win_count + ?,
                   loss_count = loss_count + ?,
                   daily_pnl = daily_pnl + ?
             WHERE date = date('now', 'localtime')
        """, (
            1 if win else 0,
            0 if win else 1,
            pnl
        ))
        conn.commit()
        
def init_db():
    with get_conn() as conn:
        cursor = conn.cursor()
        # Create tables if not exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS new_predictions (
            timestamp TEXT PRIMARY KEY,
            direction TEXT,
            confidence REAL,
            ...
        );
        """)
        conn.commit()
