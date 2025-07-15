# features/vwap_utils.py

import pandas as pd

def calculate_vwap(df):
    """
    Calculates daily VWAP and VWAP-based features.

    Features:
        - vwap
        - vwap_distance
        - vwap_distance_pct
        - above_vwap_flag
        - vwap_trend_slope_15m

    Assumes DataFrame has: ['datetime', 'high', 'low', 'close', 'volume']

    Returns:
        pd.DataFrame: With VWAP and derived features added
    """
    df = df.copy()

    # Ensure datetime is datetime type
    df["datetime"] = pd.to_datetime(df["datetime"])

    # Create a date column to reset VWAP daily
    df["date"] = df["datetime"].dt.date

    # Typical price
    df["tp"] = (df["high"] + df["low"] + df["close"]) / 3

    # Initialize VWAP column
    df["vwap"] = 0.0

    # Daily VWAP calculation
    for date in df["date"].unique():
        mask = df["date"] == date
        tp = df.loc[mask, "tp"]
        vol = df.loc[mask, "volume"]

        cum_vwap = (tp * vol).cumsum() / vol.cumsum()
        df.loc[mask, "vwap"] = cum_vwap

    # VWAP distance
    df["vwap_distance"] = df["close"] - df["vwap"]
    df["vwap_distance_pct"] = df["vwap_distance"] / df["vwap"].replace(0, 0.0001)
    df["above_vwap_flag"] = (df["close"] > df["vwap"]).astype(int)

    # VWAP slope over 15-minute window (3 bars if 5-min candles)
    df["vwap_trend_slope_15m"] = df["vwap"].diff(periods=3)

    return df
