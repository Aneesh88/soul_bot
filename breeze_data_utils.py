# breeze_data_utils.py
# Utility functions for interacting with ICICI BreezeConnect API to fetch
# BANKNIFTY futures LTP and 1-minute OHLCV bars, using the same logic
# as the old `get_latest_candle()` helper for resilience.

import logging
import time
from datetime import datetime, timedelta

import pandas as pd
from broker_utils import _ensure_session, breeze, SYMBOL_PREFIX
from trade_config import TradeConfig

log = logging.getLogger(__name__)

# --- Instrument & API settings ---
EXCHANGE_CODE    = "NFO"
PRODUCT_TYPE     = "futures"
EXPIRY_DATE      = TradeConfig.BREEZE_EXPIRY   # e.g. "26-Jun-2025"
RIGHT_FOR_INDEX  = "others"                    # for underlying futures
STRIKE_PRICE     = "0"                         # dummy for index fetch
INTERVAL         = "1minute"                   # Breeze’s interval keyword

# Lookback window & retry settings
LOOKBACK_MINUTES = 150
MAX_RETRIES      = 3
BASE_DELAY       = 1  # seconds backoff base


def fetch_latest_futures_price_breeze() -> float | None:
    _ensure_session()
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = breeze.get_quotes(
                stock_code    = SYMBOL_PREFIX,
                exchange_code = EXCHANGE_CODE,
                product_type  = PRODUCT_TYPE,
                expiry_date   = EXPIRY_DATE,
                right         = RIGHT_FOR_INDEX,
                strike_price  = STRIKE_PRICE
            )
            data = resp.get("Success") or resp.get("data") or []
            if not data or "ltp" not in data[0]:
                raise ValueError(f"No LTP: {resp}")
            return float(data[0]["ltp"])
        except Exception as e:
            log.warning(f"get_quotes attempt #{attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(BASE_DELAY * (2 ** (attempt-1)))
            else:
                log.error("All get_quotes attempts failed")
                return None


def fetch_latest_candle_breeze() -> pd.DataFrame | None:
    """
    Fetch the single most recent 1-minute candle for BANKNIFTY futures,
    using the same parameters as get_latest_candle helper.
    """
    _ensure_session()
    now = datetime.now()
    # Align to minute
    to_time = now.replace(second=0, microsecond=0)
    from_time = to_time - timedelta(minutes=1)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = breeze.get_historical_data(
                exchange_code = EXCHANGE_CODE,
                stock_code    = SYMBOL_PREFIX,
                interval      = INTERVAL,
                from_date     = from_time.strftime("%Y-%m-%dT%H:%M:%S"),
                to_date       = to_time.strftime("%Y-%m-%dT%H:%M:%S"),
                product_type  = PRODUCT_TYPE,
                expiry_date   = EXPIRY_DATE,
                right         = RIGHT_FOR_INDEX,
                strike_price  = STRIKE_PRICE
            )
            # Breeze may return list of dicts under "Success" or raw CSV
            if isinstance(resp, dict) and resp.get("Success"):
                df = pd.DataFrame(resp["Success"])
            else:
                from io import StringIO
                df = pd.read_csv(StringIO(resp))

            if df.empty:
                log.info("No candle returned")
                return None

            # Normalize
            if "datetime" in df.columns and "timestamp" not in df.columns:
                df = df.rename(columns={"datetime": "timestamp"})
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            return df[["timestamp", "open", "high", "low", "close", "volume"]]
        except Exception as e:
            log.warning(f"get_historical_data candle attempt #{attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(BASE_DELAY * (2 ** (attempt-1)))
            else:
                log.error("All candle fetch attempts failed")
                return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    print("🔍 Fetching latest BANKNIFTY futures LTP via Breeze...")
    price = fetch_latest_futures_price_breeze()
    print(f"✅ LTP: ₹{price:.2f}" if price else "❌ Failed to fetch LTP")

    print("\n🔍 Fetching most recent 1-min candle via Breeze...")
    df = fetch_latest_candle_breeze()
    if df is None or df.empty:
        print("❌ No candle data returned.")
    else:
        print(df.to_string(index=False))
