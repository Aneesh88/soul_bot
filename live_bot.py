# live_bot.py

"""
Live trading orchestrator for Deep_Om_Babaji_Om_threshold.
Drives real-time data → feature → prediction pipeline,
and triggers trade entries & exits every minute.
"""

import time
import logging
from datetime import datetime, time as dt_time
from trade_config import TradeConfig
from db import init_db
from data_fetch import data_fetch_cycle
from feature_generator import feature_generator_cycle
from predictor import predictor_cycle
from smooth_prediction import smooth_prediction_cycle
from entry_manager import entry_manager
from exit_manager import exit_manager
from telegram import send_telegram_message

# ========== Logging Setup ==========
date_str = datetime.now().strftime("%Y-%m-%d")
term_log_path = f"terminal_messages/{date_str}.txt"
error_log_path = f"errors/{date_str}.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(term_log_path),
        logging.FileHandler(error_log_path),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("main")

# ========== Constants ==========
MARKET_OPEN = dt_time(9, 15)
MARKET_CLOSE = dt_time(15, 25)
ENTRY_EXIT_START = dt_time(9, 16)  # Delay entries by 1 minute to avoid volatility

# ========== Initialize ==========
init_db()
logger.info("📡 Live bot initialized.")
send_telegram_message("🚀 Live Bot Started")

# ========== Run Loop ==========
try:
    while True:
        now = datetime.now()

        if now.time() < MARKET_OPEN:
            logger.info("⏳ Waiting for market to open...")
            time.sleep(30)
            continue

        if now.time() >= MARKET_CLOSE:
            logger.info("✅ Market closed. Exiting live bot.")
            send_telegram_message("📴 Market closed. Live bot shutting down.")
            break

        logger.info("⏱️ Running live trading cycle...")

        try:
            data_fetch_cycle()
            feature_generator_cycle()
            predictor_cycle()
            smooth_prediction_cycle()

            if now.time() >= ENTRY_EXIT_START:
                entry_manager()
                exit_manager()

        except Exception as e:
            logger.exception("❌ Exception in live loop")

        time.sleep(60)

except KeyboardInterrupt:
    logger.warning("🛑 Interrupted manually.")
    send_telegram_message("🛑 Live bot manually stopped.")
