import pandas as pd

def add_trend_regime(df, window=15, slope_threshold=0.15):
    """
    Detects trend regime based on slope of close price over rolling window.

    Features:
        - trend_regime ('uptrend', 'downtrend', 'range')
        - trend_strength (slope value)

    Parameters:
        df (pd.DataFrame): Must contain 'close'
        window (int): Number of bars to look back (e.g., 15 for 75 mins)
        slope_threshold (float): Threshold to define trend strength

    Returns:
        pd.DataFrame: With trend regime and strength columns
    """
    df = df.copy()

    # ✅ Ensure 'close' exists
    if 'close' not in df.columns:
        df['trend_strength'] = 0
        df['trend_regime'] = 'range'
        return df

    # ✅ Compute slope over rolling window
    df["trend_strength"] = df["close"].diff(periods=window)

    # Classify trend regime
    def classify_trend(slope):
        if pd.isna(slope):
            return None
        if slope > slope_threshold:
            return "uptrend"
        elif slope < -slope_threshold:
            return "downtrend"
        else:
            return "range"

    df["trend_regime"] = df["trend_strength"].apply(classify_trend)

    return df
