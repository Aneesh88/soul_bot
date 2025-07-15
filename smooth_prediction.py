import sqlite3
import pandas as pd
import numpy as np
from trade_config import BASE_DIR, TradeConfig

DB_PATH = BASE_DIR / "core_files" / "trading_data.db"
TABLE_NAME = "new_predictions"


def weighted_moving_average(series: pd.Series, window: int) -> pd.Series:
    """
    Compute a weighted moving average where more recent values
    carry higher weight.
    """
    weights = np.arange(1, window + 1)

    def _wma(x: np.ndarray) -> float:
        w = weights[-len(x):]
        return np.dot(x, w) / w.sum()

    return series.rolling(window, min_periods=1).apply(_wma, raw=True)


def apply_smoothing(
    df: pd.DataFrame,
    column: str,
    window: int,
    weighted: bool
) -> pd.Series:
    """
    Apply smoothing (simple or weighted) to the specified column.
    """
    if weighted:
        return weighted_moving_average(df[column], window)
    return df[column].rolling(window, min_periods=1).mean()


def smooth_predictions():
    """
    Load the new_predictions table, compute entry/exit smoothed confidence
    using TradeConfig settings, and overwrite the table.
    """
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)

    # Sanity check
    if df.empty or "timestamp" not in df.columns:
        print("Table is empty or missing required columns.")
        conn.close()
        return

    # Ensure ordering by timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.sort_values("timestamp", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # --- Entry smoothing ---
    if TradeConfig.ENABLE_ENTRY_SMOOTHING:
        df["entry_smoothed_long_conf"] = apply_smoothing(
            df, "long_conf",
            TradeConfig.ENTRY_SMOOTHING_WINDOW,
            TradeConfig.WEIGHTED_ENTRY_SMOOTHING
        )
        df["entry_smoothed_short_conf"] = apply_smoothing(
            df, "short_conf",
            TradeConfig.ENTRY_SMOOTHING_WINDOW,
            TradeConfig.WEIGHTED_ENTRY_SMOOTHING
        )
    else:
        df["entry_smoothed_long_conf"] = df["long_conf"]
        df["entry_smoothed_short_conf"] = df["short_conf"]

    # --- Exit smoothing ---
    if TradeConfig.ENABLE_EXIT_SMOOTHING:
        df["exit_smoothed_long_conf"] = apply_smoothing(
            df, "long_conf",
            TradeConfig.EXIT_SMOOTHING_WINDOW,
            TradeConfig.WEIGHTED_EXIT_SMOOTHING
        )
        df["exit_smoothed_short_conf"] = apply_smoothing(
            df, "short_conf",
            TradeConfig.EXIT_SMOOTHING_WINDOW,
            TradeConfig.WEIGHTED_EXIT_SMOOTHING
        )
    else:
        df["exit_smoothed_long_conf"] = df["long_conf"]
        df["exit_smoothed_short_conf"] = df["short_conf"]

    # Replace NaNs
    df.fillna(0, inplace=True)

    # Show only the latest smoothed values with rounding at display time
    latest_row = df.iloc[-1]
    print(f"\n✅ Smoothed confidence values for latest signal at {latest_row['timestamp']}:")
    print(f"   entry_smoothed_long_conf  = {latest_row['entry_smoothed_long_conf']:.3f}")
    print(f"   entry_smoothed_short_conf = {latest_row['entry_smoothed_short_conf']:.3f}")
    print(f"   exit_smoothed_long_conf   = {latest_row['exit_smoothed_long_conf']:.3f}")
    print(f"   exit_smoothed_short_conf  = {latest_row['exit_smoothed_short_conf']:.3f}\n")

    # Save back to database (with full float precision)
    df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)
    conn.close()


def smooth_prediction_cycle():
    smooth_predictions()


if __name__ == "__main__":
    smooth_predictions()
