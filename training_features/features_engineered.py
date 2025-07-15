import pandas as pd
from typing import Optional, Union


def add_feature_engineering(
    df_or_input: Union[pd.DataFrame, str],
    output_csv_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Add final engineered features to a dataset, either from an existing DataFrame or by reading a CSV.

    Parameters
    ----------
    df_or_input : pd.DataFrame or str
        Either a DataFrame already containing upstream features, or the path to a CSV file.
    output_csv_path : str, optional
        If provided, the resulting DataFrame is saved to this path as CSV.

    Returns
    -------
    pd.DataFrame
        The DataFrame with added features: 'hour', 'minute', 'event_score',
        'no_trade_zone_flag', and 'is_alpha_hour'.
    """
    # Load or copy DataFrame
    if isinstance(df_or_input, pd.DataFrame):
        df = df_or_input.copy()
    else:
        # assume string path
        df = pd.read_csv(df_or_input)

    # Ensure datetime, hour, minute columns exist
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'])
    elif 'timestamp' in df.columns:
        df['datetime'] = pd.to_datetime(df['timestamp'])
    else:
        # fallback: require date/hour/minute columns
        if {'date','hour','minute'}.issubset(df.columns):
            df['datetime'] = pd.to_datetime(
                df['date'] + ' ' +
                df['hour'].astype(str).str.zfill(2) + ':' +
                df['minute'].astype(str).str.zfill(2)
            )
        else:
            raise KeyError("Cannot find datetime information in DataFrame or CSV.")

    # Always create hour & minute
    df['hour'] = df['datetime'].dt.hour
    df['minute'] = df['datetime'].dt.minute

    # Event Score
    df['event_score'] = (
        df['body_size'] *
        df['volume_surge_magnitude'] *
        df['rolling_oi_increase_flag']
    )

    # No Trade Zone Flag
    df['no_trade_zone_flag'] = (
        (df['candle_range'] < 8) &
        (df['volume_surge_magnitude'] < 1.1) &
        (df['distance_from_vwap'].abs() < 5)
    ).astype(int)

    # Alpha Hour Flag
    df['is_alpha_hour'] = df.apply(
        lambda row: (
            (row['hour'] == 9   and row['minute'] >= 20) or
            (row['hour'] == 10) or
            (row['hour'] == 11  and row['minute'] <= 0) or
            (row['hour'] == 13  and row['minute'] >= 30) or
            (row['hour'] == 14)
        ),
        axis=1
    ).astype(int)

    # Save if path given
    if output_csv_path:
        df.to_csv(output_csv_path, index=False)
        #print(f"✅ Features added and saved to {output_csv_path}")

    return df


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python features_engineered.py <input_csv> [output_csv]")
        sys.exit(1)
    inp = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else None
    df = add_feature_engineering(inp, out)
    print(df[['datetime','event_score','no_trade_zone_flag','is_alpha_hour']].head())
    if out:
        print(f"✅ Output saved to {out}")
