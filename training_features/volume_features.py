# volume_features.py

import pandas as pd
import numpy as np

def add_volume_features(df):
    """
    Adds volume-based features to the dataframe.
    Auto-skips features if buy/sell volume or open_interest is missing.
    """

    # ==== Basic Volume Features (total volume available) ====
    df['volume_spike_flag'] = (df['volume'] > df['volume'].rolling(window=20, min_periods=1).mean() * 1.5).astype(int)
    df['volume_surge_magnitude'] = df['volume'] / (df['volume'].rolling(window=20, min_periods=1).mean())

    # ==== Buy/Sell Volume Based Features (only if buy/sell volume exists) ====
    if 'buy_volume' in df.columns and 'sell_volume' in df.columns:
        print("✅ Buy/Sell Volume detected — adding CVD features.")

        # Cumulative Volume Delta
        df['cumulative_volume_delta'] = (df['buy_volume'] - df['sell_volume']).cumsum()

        # CVD Slopes
        df['cvd_slope_5min'] = df['cumulative_volume_delta'].diff(periods=5)
        df['cvd_slope_15min'] = df['cumulative_volume_delta'].diff(periods=15)

        # Buy/Sell Volume Difference
        df['buy_sell_volume_diff'] = df['buy_volume'] - df['sell_volume']

    else:

        print("")

    # ==== Open Interest (OI) Based Features (only if open_interest exists) ====
    if 'open_interest' in df.columns:
        #print("✅ Open Interest detected — adding OI features.")

        df['oi_change_1min'] = df['open_interest'].diff()
        df['oi_change_5min'] = df['open_interest'].diff(periods=5)

        # Rolling OI trend flags
        df['rolling_oi_increase_flag'] = (df['open_interest'].diff(periods=5) > 0).astype(int)
        df['rolling_oi_decrease_flag'] = (df['open_interest'].diff(periods=5) < 0).astype(int)

        # Price-OI Divergence
        price_change_5min = df['close'].diff(periods=5)
        oi_change_5min = df['open_interest'].diff(periods=5)
        df['price_oi_divergence_flag'] = ((price_change_5min * oi_change_5min) < 0).astype(int)

        # Price-Volume-OI Confluence
        volume_change_5min = df['volume'].diff(periods=5)
        df['price_volume_oi_confluence_flag'] = (
            (np.sign(price_change_5min) == np.sign(oi_change_5min)) &
            (np.sign(price_change_5min) == np.sign(volume_change_5min))
        ).astype(int)

    else:
        print("⚠️ Open Interest not detected — skipping OI features.")

    return df
