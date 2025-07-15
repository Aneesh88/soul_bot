# telegram.py
# Send entry/exit notifications via Telegram bot, with HTML formatting.

import logging
import requests
from trade_config import TradeConfig
from broker_utils import SYMBOL_PREFIX

log = logging.getLogger(__name__)

TELEGRAM_URL = f"https://api.telegram.org/bot{TradeConfig.TELEGRAM_TOKEN}/sendMessage"


def send_message(text: str):
    """
    Send a Telegram message with HTML parsing.
    401s (invalid token) will be logged once and then swallowed.
    """
    try:
        resp = requests.get(TELEGRAM_URL, params={
            "chat_id":    TradeConfig.TELEGRAM_CHAT_ID,
            "text":       text,
            "parse_mode": "HTML"
        }, timeout=5)
        if resp.status_code == 401:
            log.error("Telegram unauthorized (401) – check your TELEGRAM_TOKEN")
            return
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error("Failed to send Telegram message: %s", e)


def send_entry_notification(trade: dict) -> None:
    """
    Format and send an ENTRY notification.
    """
    tid        = trade.get('trade_number') or trade.get('entry_order_id')
    direction  = trade.get('direction', 'LONG')
    right      = 'CE' if direction == 'LONG' else 'PE'
    instrument = f"{SYMBOL_PREFIX}{trade.get('strike')}{right}"

    raw_conf = trade.get('raw_confidence') or 0.0
    sm_conf  = trade.get('smoothed_confidence') or 0.0
    entry_opt = trade.get('entry_price_option') or 0.0

    msg = (
        f"<b>ENTRY:{tid}</b>\n"
        f"📅 <b>Date/Time:</b> {trade.get('entry_time')}\n"
        f"📘 <b>Direction:</b> {'CALL' if direction == 'LONG' else 'PUT'}\n"
        f"💡 <b>Raw Conf:</b> {raw_conf:.2f}\n"
        f"🔄 <b>Smoothed Conf:</b> {sm_conf:.2f}\n"
        f"🎯 <b>Instrument:</b> {instrument}\n"
        f"💵 <b>Entry Price:</b> ₹{entry_opt:.2f}\n"
        f"📦 <b>Qty:</b> {trade.get('quantity')}\n"
        f"🛑 <b>Index SL:</b> {trade.get('fut_index_sl_level')}\n"
        f"✅ <b>Index TP:</b> {trade.get('fut_index_tp_level')}"
    )
    send_message(msg)


def send_exit_notification(trade: dict, day_pnl: float) -> None:
    """
    Format and send an EXIT notification.
    """
    tid        = trade.get('trade_number') or trade.get('entry_order_id')
    direction  = trade.get('direction', 'LONG')
    right      = 'CE' if direction == 'LONG' else 'PE'
    instrument = f"{SYMBOL_PREFIX}{trade.get('strike')}{right}"

    option_pnl = trade.get('option_pnl') or 0.0
    index_pnl  = trade.get('index_pnl') or 0.0
    conf       = trade.get('confidence') or 0.0
    entry_opt  = trade.get('entry_price_option') or 0.0
    exit_opt   = trade.get('exit_price_option') or 0.0

    emoji = "✅ WIN" if (option_pnl + index_pnl) >= 0 else "❌ LOSS"

    msg = (
        f"<b>EXIT:{tid} {emoji}</b>\n"
        f"📅 <b>Date/Time:</b> {trade.get('exit_time')}\n"
        f"🎯 <b>Instrument:</b> {instrument}\n"
        f"💡 <b>Confidence:</b> {conf:.2f}\n"
        f"💵 <b>Entry:</b> ₹{entry_opt:.2f}\n"
        f"💰 <b>Exit:</b> ₹{exit_opt:.2f}\n"
        f"📦 <b>Qty:</b> {trade.get('quantity')}\n"
        f"📊 <b>Option P&L:</b> ₹{option_pnl:.2f}\n"
        f"📊 <b>Index P&L:</b> ₹{index_pnl:.2f}\n"
        f"📈 <b>Day P/L:</b> ₹{(day_pnl or 0.0):.2f}"
    )
    send_message(msg)


def send_heartbeat():
    """Optional: Send a periodic heartbeat."""
    send_message("🟢 Bot is alive and monitoring trades.")
