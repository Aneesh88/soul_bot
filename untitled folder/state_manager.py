# state_manager.py – now uses DB instead of JSON
import sqlite3
import logging
from typing import List, Dict, Any
from datetime import date
from db import get_conn

log = logging.getLogger(__name__)

# === LIVE TRADES ===

def load_live_trades() -> List[Dict[str, Any]]:
    """Load open trades (status='OPEN') from the database."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM live_trade_details
            WHERE status = 'OPEN'
        """)
        rows = cur.fetchall()
        columns = [col[0] for col in cur.description]
        return [dict(zip(columns, row)) for row in rows]


def save_live_trades(state: List[Dict[str, Any]]) -> None:
    """
    Replace all open trades with new state.
    Useful for rare manual resets.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM live_trade_details WHERE status = 'OPEN'")
        for trade in state:
            cur.execute("""
                INSERT INTO live_trade_details (
                    trade_number, timestamp, entry_time, direction,
                    confidence, instrument, quantity,
                    entry_index_price, entry_price_option,
                    status, exit_index_price, option_pnl, index_pnl,
                    fut_index_sl_level, fut_index_tp_level
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade["trade_number"],
                trade["timestamp"],
                trade.get("entry_time"),
                trade["direction"],
                trade["confidence"],
                trade["instrument"],
                trade["quantity"],
                trade["entry_index_price"],
                trade["entry_price_option"],
                trade["status"],
                trade.get("exit_index_price"),
                trade.get("option_pnl"),
                trade.get("index_pnl"),
                trade.get("fut_index_sl_level"),
                trade.get("fut_index_tp_level"),
            ))
        conn.commit()



# === TRADE STATE ===

def load_trade_state() -> Dict[str, Any]:
    """Load today's trade state, or initialize a new one."""
    today_str = date.today().isoformat()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM daily_trade_state WHERE date = ?", (today_str,))
        row = cur.fetchone()
        if row:
            columns = [col[0] for col in cur.description]
            return dict(zip(columns, row))
        else:
            # Create initial state for today
            initial = {
                "date": today_str,
                "last_trade_number": 0,
                "active_trade_count": 0,
                "closed_trade_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "daily_pnl": 0.0
            }
            cur.execute("""
                INSERT INTO daily_trade_state (
                    date, last_trade_number, active_trade_count,
                    closed_trade_count, win_count, loss_count, daily_pnl
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                today_str, 0, 0, 0, 0, 0, 0.0
            ))
            conn.commit()
            return initial

def save_trade_state(state: Dict[str, Any]) -> None:
    """Update today's trade state."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE daily_trade_state SET
                last_trade_number = ?,
                active_trade_count = ?,
                closed_trade_count = ?,
                win_count = ?,
                loss_count = ?,
                daily_pnl = ?
            WHERE date = ?
        """, (
            state["last_trade_number"],
            state["active_trade_count"],
            state["closed_trade_count"],
            state["win_count"],
            state["loss_count"],
            state["daily_pnl"],
            state["date"]
        ))
        conn.commit()
