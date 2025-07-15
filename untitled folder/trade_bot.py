# trade_bot.py
"""
Central orchestrator: drives DB-backed data→feature→predict pipeline every minute,
handles trade entries/exits on separate cadences, and sends periodic heartbeats.

Console will only show:
  - ⏱️ … Prediction …   (via logger 'console')
  - Trade#… ENTRY / EXIT…   (via logger 'trade_manager')
  - Any ERRORs
Everything else is routed to the file handlers only.
"""

import os
import threading
import time
import logging
from datetime import datetime, time as dt_time

from trade_config import TradeConfig
from db import init_db
from data_fetch import data_fetch_cycle
from feature_generator import feature_generator_cycle
from predictor import predictor_cycle
from smooth_prediction import smooth_prediction_cycle
import trade_manager      # entry_manager() & exit_manager()
import telegram           # for heartbeat_loop

# 0) Ensure DB schema exists
init_db()

# 1) Prepare log dirs
date_str = datetime.now().strftime("%Y-%m-%d")
term_dir = "terminal_messages"
err_dir  = "errors"
os.makedirs(term_dir, exist_ok=True)
os.makedirs(err_dir, exist_ok=True)

# 2) Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# 3) File handler for all INFO+ → terminal_messages/YYYY-MM-DD.txt
term_path = os.path.join(term_dir, f"{date_str}.txt")
term_handler = logging.FileHandler(term_path)
term_handler.setLevel(logging.INFO)
term_handler.setFormatter(fmt)
root_logger.addHandler(term_handler)

# 4) File handler for ERROR+ → errors/YYYY-MM-DD.txt
err_path = os.path.join(err_dir, f"{date_str}.txt")
err_handler = logging.FileHandler(err_path)
err_handler.setLevel(logging.ERROR)
err_handler.setFormatter(fmt)
root_logger.addHandler(err_handler)

# 5) Console handler, but filtered
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(fmt)

class ConsoleFilter(logging.Filter):
    """
    Allow through:
      - Any ERROR or higher
      - Anything logged under logger name 'console'
      - Anything logged under logger name 'trade_manager'
    All other records are dropped from the console.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.ERROR:
            return True
        if record.name == "console":
            return True
        if record.name == "trade_manager":
            return True
        return False

console_handler.addFilter(ConsoleFilter())
root_logger.addHandler(console_handler)


def pipeline_loop():
    """
    Every minute, run:
      1) data_fetch_cycle      → inserts into 'bars'
      2) feature_generator_cycle → inserts into 'features'
      3) predictor_cycle        → inserts into 'new_predictions'
      4) smooth_prediction_cycle → updates 'new_predictions' with smoothed fields
    (No time gating here – always runs once per minute.)
    """
    log = logging.getLogger("pipeline_loop")
    log.info("Starting pipeline loop")

    while True:
        for fn in (data_fetch_cycle, feature_generator_cycle, predictor_cycle, smooth_prediction_cycle):
            try:
                fn()
            except Exception:
                log.exception(f"pipeline_loop: {fn.__name__} failed")
        time.sleep(60 - datetime.now().second)


def entry_loop():
    """
    Run entry_manager once every 60 seconds, but only within the entry window.
    """
    log = logging.getLogger("entry_loop")
    log.info("Starting entry loop (60s cadence)")
    start = dt_time.fromisoformat(TradeConfig.ENTRY_START)
    end   = dt_time.fromisoformat(TradeConfig.ENTRY_END)

    while True:
        now = datetime.now().time()
        if start <= now <= end:
            try:
                trade_manager.entry_manager()
            except Exception:
                log.exception("entry_loop: entry_manager failed")
        else:
            log.debug("Outside entry window; skipping entry_manager")
        time.sleep(60 - datetime.now().second)


def exit_loop():
    """
    Run exit_manager every 10 seconds to catch SL/TP/EOD quickly.
    """
    log = logging.getLogger("exit_loop")
    log.info("Starting exit loop (10s cadence)")
    while True:
        try:
            trade_manager.exit_manager()
        except Exception:
            log.exception("exit_loop: exit_manager failed")
        time.sleep(10)


def heartbeat_loop():
    """
    Send hourly heartbeats via Telegram.
    """
    log = logging.getLogger("heartbeat_loop")
    log.info("Starting heartbeat loop")
    while True:
        try:
            telegram.send_message(f"Heartbeat: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception:
            log.exception("heartbeat_loop: send_message failed")
        time.sleep(3600)


if __name__ == "__main__":
    # Spawn loops as daemon threads
    for target in (pipeline_loop, entry_loop, exit_loop, heartbeat_loop):
        t = threading.Thread(target=target, daemon=True)
        t.start()

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Shutting down trade_bot")
