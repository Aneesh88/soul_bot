# features/volume_signals.py

import pandas as pd

def add_volume_features(df, rolling_window=10):
    """
    Adds volume-based features for spike and direction bias.

    Features added:
        - rolling_avg_vol_10
        - volume_spike
        - delta_volume_sign
        - cum_delta_5

    Parameters:
        df (pd.DataFrame): DataFrame with 'volume', 'close', 'open'
        rolling_window (int): Window size for rolling volume averages

    Returns:
        pd.DataFrame: With new volume features added
    """
    df = df.copy()

    # Rolling average volume
    df["rolling_avg_vol_10"] = df["volume"].rolling(window=rolling_window).mean()

    # Volume spike ratio
    df["volume_spike"] = df["volume"] / df["rolling_avg_vol_10"].replace(0, 1)

    # Directional delta volume (approximated as buyer/seller aggression)
    df["direction"] = df["close"] - df["open"]
    df["delta_volume_sign"] = df["volume"] * df["direction"].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))

    # Rolling sum of delta volume
    df["cum_delta_5"] = df["delta_volume_sign"].rolling(window=5).sum()

    return df
