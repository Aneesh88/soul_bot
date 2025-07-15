import pandas as pd
import numpy as np

# ==== Your Existing Functions ====

def cvd_slope_10m(df: pd.DataFrame, window: int = 10) -> pd.Series:
    """Calculates the slope of the Cumulative Volume Delta over the given window."""
    cvd = (df['buy_volume'] - df['sell_volume']).cumsum()
    slope = cvd.diff(window) / window
    return slope.rename("cvd_slope_10m")

def ema_50_15m_above_ema_200_15m(df_15m: pd.DataFrame) -> pd.Series:
    """Compares EMA-50 and EMA-200 on 15-minute timeframe."""
    ema_50 = df_15m['close'].ewm(span=50, adjust=False).mean()
    ema_200 = df_15m['close'].ewm(span=200, adjust=False).mean()
    return (ema_50 > ema_200).astype(int).rename("ema_50_15m_above_ema_200_15m")

def entry_direction_aligned_with_trend(entry_dir: pd.Series, trend_dir: pd.Series) -> pd.Series:
    """Returns 1 if entry direction matches trend direction, else 0."""
    return (entry_dir == trend_dir).astype(int).rename("entry_direction_aligned_with_trend")

# ==== Main Aggregator ====

def add_trend_features(df: pd.DataFrame) -> pd.DataFrame:
    """Adds trend-related features to the dataframe using existing helper functions."""

    df = df.copy()

    # ✅ CVD Slope Feature (if volumes exist)
    if 'buy_volume' in df.columns and 'sell_volume' in df.columns:
        df['cvd_slope_10m'] = cvd_slope_10m(df)
    else:
        df['cvd_slope_10m'] = 0  # fallback if not present in forward test

    # ✅ EMA 15min Feature (resampling needs datetime index)
    try:
        df_resample = df.set_index('datetime')[['open', 'high', 'low', 'close', 'volume']]
        df_15m = df_resample.resample('15min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })

        ema_cross_flag = ema_50_15m_above_ema_200_15m(df_15m)
       # Proper datetime-based alignment using merge_asof
        ema_cross_flag = ema_cross_flag.reset_index().rename(columns={'index': 'datetime'})
        df = pd.merge_asof(df.sort_values('datetime'), ema_cross_flag.sort_values('datetime'),
                   on='datetime', direction='backward')


    except Exception as e:
        print(f"⚠️ EMA Crossover Feature generation failed: {e}")
        df['ema_50_15m_above_ema_200_15m'] = 0  # fallback

    return df
