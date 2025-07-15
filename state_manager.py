# state_manager.py – DB-based trade state manager
import sqlite3
import logging
from typing import List, Dict, Any
from datetime import date
from db import get_conn

logger = logging.getLogger("state_manager")

# === LIVE TRADE STATE ===

def load_live_trades() -> List[Dict[str, Any]]:
    """Load open trades (status = 'OPEN') from DB."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM live_trade_details
             WHERE status = 'OPEN'
            """
        )
        rows = cur.fetchall()
        columns = [col[0] for col in cur.description]
        return [dict(zip(columns, row)) for row in rows]


def save_live_trades(state: List[Dict[str, Any]]) -> None:
    """
    Save entire list of open trades (overwrite OPENs).
    Useful for rare full manual updates.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM live_trade_details WHERE status = 'OPEN'")
        for trade in state:
            cur.execute(
                """
                INSERT INTO live_trade_details (
                    trade_number, timestamp, entry_time, direction,
                    confidence, instrument, quantity,
                    entry_index_price, entry_price_option,
                    status, exit_index_price, option_pnl, index_pnl,
                    fut_index_sl_level, fut_index_tp_level, strike, raw_confidence
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade.get("trade_number"),
                    trade.get("timestamp"),
                    trade.get("entry_time"),
                    trade.get("direction"),
                    trade.get("confidence"),
                    trade.get("instrument"),
                    trade.get("quantity"),
                    trade.get("entry_index_price"),
                    trade.get("entry_price_option"),
                    trade.get("status"),
                    trade.get("exit_index_price"),
                    trade.get("option_pnl"),
                    trade.get("index_pnl"),
                    trade.get("fut_index_sl_level"),
                    trade.get("fut_index_tp_level"),
                    trade.get("strike"),
                    trade.get("raw_confidence")
                )
            )
        conn.commit()

# === ALIASES FOR COMPATIBILITY ===
get_live_trades = load_live_trades
update_live_trades = save_live_trades

# === DAILY TRADE STATE ===

def load_trade_state() -> Dict[str, Any]:
    """Load or initialize today's trade state (from daily_trade_state table)."""
    today_str = date.today().isoformat()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM daily_trade_state WHERE date = ?", (today_str,))
        row = cur.fetchone()
        if row:
            columns = [col[0] for col in cur.description]
            return dict(zip(columns, row))
        else:
            # First call today, insert blank state
            initial_state = {
                "date": today_str,
                "last_trade_number": 0,
                "active_trade_count": 0,
                "closed_trade_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "daily_pnl": 0.0
            }
            cur.execute(
                """
                INSERT INTO daily_trade_state (
                    date, last_trade_number, active_trade_count,
                    closed_trade_count, win_count, loss_count, daily_pnl
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    initial_state["date"],
                    initial_state["last_trade_number"],
                    initial_state["active_trade_count"],
                    initial_state["closed_trade_count"],
                    initial_state["win_count"],
                    initial_state["loss_count"],
                    initial_state["daily_pnl"]
                )
            )
            conn.commit()
            return initial_state


def save_trade_state(state: Dict[str, Any]) -> None:
    """Update today's trade state row in DB."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE daily_trade_state SET
                last_trade_number = ?,
                active_trade_count = ?,
                closed_trade_count = ?,
                win_count = ?,
                loss_count = ?,
                daily_pnl = ?
             WHERE date = ?
            """,
            (
                state["last_trade_number"],
                state["active_trade_count"],
                state["closed_trade_count"],
                state["win_count"],
                state["loss_count"],
                state["daily_pnl"],
                state["date"]
            )
        )
        conn.commit()

# === DAILY COUNT UTILITIES ===

def get_daily_trade_count() -> int:
    """Return today's total number of trades placed."""
    state = load_trade_state()
    return state.get("last_trade_number", 0)


def increment_trade_count() -> None:
    """Increment today's trade count by one."""
    state = load_trade_state()
    state["last_trade_number"] = state.get("last_trade_number", 0) + 1
    save_trade_state(state)
