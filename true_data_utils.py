# true_data_utils.py
# Utility functions for interacting with TrueData API

import logging
import requests
import time
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
from trade_config import TradeConfig

log = logging.getLogger(__name__)

# How many minutes of history to fetch each cycle (must cover potential gaps)
LOOKBACK_MINUTES = 150


def get_token() -> str:
    """
    Obtain OAuth token from TrueData using TradeConfig credentials.
    """
    url = "https://auth.truedata.in/token"
    payload = {
        "username": TradeConfig.TD_USER,
        "password": TradeConfig.TD_PASS,
        "grant_type": "password"
    }
    resp = requests.post(url, data=payload)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    log.debug("Obtained TrueData token")
    return token


def fetch_latest_ohlcv() -> pd.DataFrame | None:
    """
    Fetch the latest LOOKBACK_MINUTES of 1-minute OHLCV bars from TrueData.
    Returns a DataFrame with columns matching TradeConfig.BAR_COLS,
    or None if the fetch failed or returned no data.
    """
    for attempt in range(1, TradeConfig.TRUE_DATA_MAX_RETRIES + 1):
        try:
            token = get_token()
            end_time = datetime.now()
            start_time = end_time - timedelta(minutes=LOOKBACK_MINUTES)

            url = "https://history.truedata.in/getbars"
            headers = {"Authorization": f"Bearer {token}"}
            params = {
                "symbol":   TradeConfig.TD_SYMBOL,
                "from":     start_time.strftime("%y%m%dT%H:%M:%S"),
                "to":       end_time.strftime("%y%m%dT%H:%M:%S"),
                "interval": TradeConfig.TD_INTERVAL,
                "response": "csv"
            }
            resp = requests.get(url, headers=headers, params=params)
            if resp.status_code == 401:
                log.warning("TrueData token expired, retrying...")
                token = get_token()
                headers["Authorization"] = f"Bearer {token}"
                resp = requests.get(url, headers=headers, params=params)

            resp.raise_for_status()
            csv_text = resp.text.strip()
            if not csv_text:
                log.info("TrueData returned empty CSV")
                return None

            df = pd.read_csv(StringIO(csv_text))
            # Normalize column names
            if 'datetime' not in df.columns and 'timestamp' in df.columns:
                df.rename(columns={'timestamp': 'datetime'}, inplace=True)

            required = {'datetime', 'open', 'high', 'low', 'close', 'volume', 'oi'}
            if not required.issubset(df.columns):
                log.error(f"TrueData missing cols: {set(df.columns)}")
                return None

            df = df[['datetime', 'open', 'high', 'low', 'close', 'volume', 'oi']].dropna()
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.rename(columns={'datetime': 'timestamp', 'oi': 'open_interest'}, inplace=True)
            return df

        except Exception as e:
            log.error(f"Attempt {attempt} fetch failed: {e}", exc_info=True)
            if attempt < TradeConfig.TRUE_DATA_MAX_RETRIES:
                time.sleep(TradeConfig.TRUE_DATA_RETRY_DELAY)
            else:
                log.error("All TrueData fetch attempts failed")
                return None


def get_banknifty_futures_price() -> float | None:
    """
    Fetch the latest BANKNIFTY futures index price by pulling the most
    recent 'close' from the OHLCV DataFrame.
    Returns the last 'close' price or None if unavailable.
    """
    df = fetch_latest_ohlcv()
    if df is None or df.empty:
        return None
    price = float(df.iloc[-1]['close'])
    log.debug(f"Latest BANKNIFTY futures index price: {price}")
    return price
