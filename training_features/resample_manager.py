import pandas as pd

def resample_all_timeframes(df_1m: pd.DataFrame) -> dict:
    """
    Resamples 1-minute OHLCV data to 15-minute and 30-minute intervals.
    
    Assumes df_1m has datetime index and columns: 
    ['open', 'high', 'low', 'close', 'volume']

    Returns:
        dict: {
            "1m": original 1-minute DataFrame,
            "15m": 15-minute resampled OHLCV DataFrame,
            "30m": 30-minute resampled OHLCV DataFrame
        }
    """
    df_15m = df_1m.resample("15T").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    }).dropna()

    df_30m = df_1m.resample("30T").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    }).dropna()

    return {
        "1m": df_1m,
        "15m": df_15m,
        "30m": df_30m
    }
