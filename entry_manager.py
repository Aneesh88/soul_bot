# entry_manager.py

from datetime import datetime, timedelta
from state_manager import (
    get_live_trades, update_live_trades,
    get_daily_trade_count, increment_trade_count
)
from trade_config import TradeConfig
from broker_utils import entry_order
from db import load_latest_prediction_row
from state_manager import get_live_trades

import logging
logger = logging.getLogger("entry")

def is_recent(timestamp: datetime, max_age_sec: int = 120) -> bool:
    """Check if the signal is fresh (within allowed max age)."""
    return (datetime.now() - timestamp).total_seconds() <= max_age_sec

def is_valid_entry_signal(row) -> str | None:
    """Determine if the row qualifies for LONG or SHORT entry."""
    if not is_recent(row["timestamp"], TradeConfig.ENTRY_MAX_SIGNAL_AGE):
        return None
    if row["entry_smoothed_long_conf"] >= TradeConfig.LONG_TH:
        return "LONG"
    elif row["entry_smoothed_short_conf"] >= TradeConfig.SHORT_TH:
        return "SHORT"
    return None

def build_trade_object(row, direction: str) -> dict:
    """Build the trade dictionary object."""
    return {
        "timestamp": row["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
        "entry_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "direction": direction,
        "entry_price": row["close"],
        "entry_conf": row["entry_smoothed_long_conf"] if direction == "LONG" else row["entry_smoothed_short_conf"],
        "symbol": TradeConfig.SYMBOL,
        "qty": TradeConfig.DEFAULT_QTY,
        "status": "OPEN",
        "exit_time": None,
        "exit_price": None,
        "pnl": None,
        "exit_reason": None
    }

def entry_manager():
    """Run every minute. Handles trade entries based on the simulator rules."""
    try:
        row = load_latest_prediction_row()
        if row is None:
            logger.info("No predictions available yet.")
            return

        signal = is_valid_entry_signal(row)
        if not signal:
            return

        live_trades = get_live_trades()
        active_trades = [t for t in live_trades if t["status"] == "OPEN"]

        if len(active_trades) >= TradeConfig.MAX_CONCURRENT_TRADES:
            logger.info(f"🚫 Max concurrent trades reached.")
            return

        daily_count = get_daily_trade_count()
        if daily_count >= TradeConfig.MAX_TRADES_PER_DAY:
            logger.info(f"🚫 Daily trade limit reached.")
            return

        # Place broker order
        entry_price = row["close"]
        success, order_id = entry_order(direction=signal, price=entry_price, qty=TradeConfig.DEFAULT_QTY)

        if success:
            trade = build_trade_object(row, signal)
            trade["order_id"] = order_id
            live_trades.append(trade)
            update_live_trades(live_trades)
            increment_trade_count()
            logger.info(f"✅ TRADE ENTRY [{signal}] at {entry_price} | conf={trade['entry_conf']:.3f}")
        else:
            logger.warning(f"❌ Order rejected for {signal} at {entry_price}")

    except Exception as e:
        logger.exception(f"❌ Error in entry_manager: {e}")
