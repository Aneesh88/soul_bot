# features/open_interest.py

import pandas as pd

def add_oi_features(df):
    """
    Adds open interest (OI) based features to detect build-up and unwinding activity.

    Features added:
        - oi_change
        - oi_change_5
        - price_up_oi_up       (Long Buildup)
        - price_down_oi_up     (Short Buildup)
        - price_up_oi_down     (Short Covering)
        - price_down_oi_down   (Long Unwinding)

    Parameters:
        df (pd.DataFrame): Must contain 'oi', 'close', 'open'

    Returns:
        pd.DataFrame: With new OI-related features added
    """
    df = df.copy()

    # OI changes
    df["oi_change"] = df["oi"].diff()
    df["oi_change_5"] = df["oi"].diff(periods=5)

    # Price direction
    df["price_up"] = (df["close"] > df["open"]).astype(int)
    df["price_down"] = (df["close"] < df["open"]).astype(int)

    # Behavior flags
    df["price_up_oi_up"] = ((df["price_up"] == 1) & (df["oi_change"] > 0)).astype(int)       # Long buildup
    df["price_down_oi_up"] = ((df["price_down"] == 1) & (df["oi_change"] > 0)).astype(int)   # Short buildup
    df["price_up_oi_down"] = ((df["price_up"] == 1) & (df["oi_change"] < 0)).astype(int)     # Short covering
    df["price_down_oi_down"] = ((df["price_down"] == 1) & (df["oi_change"] < 0)).astype(int) # Long unwinding

    return df
