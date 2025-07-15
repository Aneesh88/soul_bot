"""
Test script for the `ema_filter_15` logic in `feature_generator.py`, with inclusive start and exclusive end to capture full-day bars.

This module:
  1. Loads raw 1-min bars from the `bars` table in your SQLite DB for a specified date range.
  2. Invokes the private `_compute_ema_filter_15` function to compute the EMA20/EMA50 regime flag.
  3. Prints the timestamp and computed `ema_filter_15` values.
  4. Optionally saves the output to CSV.

Usage:
  - Adjust `FROM_DATE` and `TO_DATE` below (inclusive for start, exclusive for end).
  - (Optional) Set `OUTPUT_CSV` to a filepath to persist results.
  - Run: `python test_ema_filter_15.py`

Date format: "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS"
"""
import pandas as pd
from db import get_conn
from feature_generator import _compute_ema_filter_15

# -----------------------------------------------------------------------------
# Configure your date range here:
FROM_DATE = "2025-06-25"  # inclusive start
TO_DATE   = "2025-06-25"  # inclusive day; end is exclusive via next-day logic
# -----------------------------------------------------------------------------
# (Optional) filepath to save results; set to None to skip CSV output
OUTPUT_CSV = None  # e.g. "/path/to/ema_filter_test.csv"
# -----------------------------------------------------------------------------

def main():
    # Use exclusive end: next-day at midnight
    next_day = pd.to_datetime(TO_DATE) + pd.Timedelta(days=1)
    end_date = next_day.strftime("%Y-%m-%d")

    query = '''
    SELECT timestamp, open, high, low, close
    FROM bars
    WHERE timestamp >= ?
      AND timestamp < ?
    ORDER BY timestamp
    '''

    with get_conn() as conn:
        df_bars = pd.read_sql(query, conn, params=(FROM_DATE, end_date))

    if df_bars.empty:
        print(f"No bars found between {FROM_DATE} and (before) {end_date}.")
        return

    # Compute ema_filter_15 flag
    df_bars['ema_filter_15'] = _compute_ema_filter_15(df_bars)

    # Display results
    output_df = df_bars[['timestamp', 'ema_filter_15']]
    print(output_df.to_string(index=False))

    # Optional CSV export
    if OUTPUT_CSV:
        output_df.to_csv(OUTPUT_CSV, index=False)
        print(f"\nSaved {len(output_df)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
