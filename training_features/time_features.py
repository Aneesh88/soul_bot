import pandas as pd

# ==== Your Existing Functions ====

def extract_hour(df: pd.DataFrame) -> pd.Series:
    """
    Extracts hour from the 'time' column.
    """
    return pd.to_datetime(df['time'], format='%H:%M').dt.hour.rename('hour')

def extract_minute(df: pd.DataFrame) -> pd.Series:
    """
    Extracts minute from the 'time' column.
    """
    return pd.to_datetime(df['time'], format='%H:%M').dt.minute.rename('minute')

def calculate_minute_of_day(df: pd.DataFrame) -> pd.Series:
    """
    Calculates minutes passed since midnight based on 'time' column.
    """
    hour = pd.to_datetime(df['time'], format='%H:%M').dt.hour
    minute = pd.to_datetime(df['time'], format='%H:%M').dt.minute
    return (hour * 60 + minute).rename('minute_of_day')

def is_expiry_week(df: pd.DataFrame) -> pd.Series:
    """
    Flags if the current date falls in the expiry week (assumed last week of month).
    """
    date_series = pd.to_datetime(df['date'])
    return (date_series.dt.day >= 23).astype(int).rename('expiry_week_flag')

def get_weekday(df: pd.DataFrame) -> pd.Series:
    """
    Returns the day of week (0=Monday, ..., 6=Sunday).
    """
    return pd.to_datetime(df['date']).dt.weekday.rename('weekday')

# ==== Main Aggregator Function for Time Features ====

def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds time-based features to the dataframe using existing helper functions.
    Ensures 'time' and 'date' columns are available for feature extraction.
    """

    # 🛠 Fallback: Create 'time' and 'date' from 'datetime' if missing
    if 'time' not in df.columns:
        df['time'] = pd.to_datetime(df['datetime']).dt.strftime('%H:%M')
    if 'date' not in df.columns:
        df['date'] = pd.to_datetime(df['datetime']).dt.date.astype(str)

    # 🔧 Apply all time-based feature generators
    df['hour'] = extract_hour(df)
    df['minute'] = extract_minute(df)
    df['minute_of_day'] = calculate_minute_of_day(df)
    df['expiry_week_flag'] = is_expiry_week(df)
    df['weekday'] = get_weekday(df)

    return df
