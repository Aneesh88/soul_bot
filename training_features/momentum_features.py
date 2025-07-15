import pandas as pd
import numpy as np

# ==== Your Existing Functions (example MACD calculation etc.) ====

def calculate_macd_histogram(df: pd.DataFrame) -> pd.Series:
    """
    Calculate MACD Histogram for given close prices.
    """
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=9, adjust=False).mean()
    histogram = macd - signal
    return histogram.rename('macd_histogram_30m')

# ==== NEW: Main Aggregator Function for Momentum Features ====

def add_momentum_features(df):
    """
    Adds momentum-related features to the dataframe using existing helper functions.
    """

    # Example: Add MACD Histogram (can extend later for RSI, Stochastic, etc.)
    df['macd_histogram_30m'] = calculate_macd_histogram(df)

    return df
