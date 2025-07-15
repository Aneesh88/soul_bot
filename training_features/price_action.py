# features/price_action.py

import pandas as pd

def add_price_action_features(df):
    """
    Adds price action features like body size, wick length, and direction.
    Assumes input DataFrame has at least: ['open', 'high', 'low', 'close']

    Returns:
        pd.DataFrame with new columns:
            - body_size
            - wick_top
            - wick_bottom
            - candle_range
            - is_bullish
            - is_bearish
            - direction
            - body_ratio
            - wick_ratio
    """
    df = df.copy()

    # Basic candle shape features
    df["body_size"] = abs(df["close"] - df["open"])
    df["candle_range"] = df["high"] - df["low"]
    df["wick_top"] = df["high"] - df[["open", "close"]].max(axis=1)
    df["wick_bottom"] = df[["open", "close"]].min(axis=1) - df["low"]

    # Candle direction
    df["is_bullish"] = (df["close"] > df["open"]).astype(int)
    df["is_bearish"] = (df["close"] < df["open"]).astype(int)
    df["direction"] = df["close"] - df["open"]

    # Candle structure ratios
    df["body_ratio"] = df["body_size"] / df["candle_range"].replace(0, 0.0001)
    df["wick_ratio"] = (df["wick_top"] + df["wick_bottom"]) / df["candle_range"].replace(0, 0.0001)

    return df
