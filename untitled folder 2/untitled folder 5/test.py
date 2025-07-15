import os
import pandas as pd
from db import get_conn

# Only consider data from this date onwards
START_FILTER_DATE = pd.Timestamp('2024-01-01')

def compare_predictions(csv_path: str = '/Users/aneeshviswanathan/Desktop/Om_Babaji_Om_threshold/model_predictions.csv', \
                        output_path: str = '/Users/aneeshviswanathan/Desktop/Om_Babaji_Om_threshold/prediction_mismatches.csv') -> None:
    """
    Compares timestamp, direction, and confidence between a CSV file
    and the 'new_predictions' table in the database, considering only data
    from START_FILTER_DATE onwards. Reports and saves mismatches.

    Parameters:
    - csv_path: Path to the model_predictions.csv file.
    - output_path: Path to save the mismatches report CSV.
    """
    # Load CSV
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    df_csv = pd.read_csv(csv_path, parse_dates=['timestamp'])
    df_csv = df_csv[['timestamp', 'direction', 'confidence']].copy()
    # Filter by start date
    df_csv = df_csv[df_csv['timestamp'] >= START_FILTER_DATE]
    df_csv.rename(columns={'direction': 'direction_csv', 'confidence': 'confidence_csv'}, inplace=True)

    # Load database table
    with get_conn() as conn:
        df_db = pd.read_sql(
            "SELECT timestamp, direction, confidence FROM new_predictions", conn,
            parse_dates=['timestamp']
        )
    # Filter by start date
    df_db = df_db[df_db['timestamp'] >= START_FILTER_DATE]
    df_db = df_db[['timestamp', 'direction', 'confidence']].copy()
    df_db.rename(columns={'direction': 'direction_db', 'confidence': 'confidence_db'}, inplace=True)

    # Merge on timestamp
    merged = pd.merge(
        df_csv,
        df_db,
        on='timestamp',
        how='outer',
        indicator=True
    )

    # Identify mismatches
    mismatches = merged[
        (merged['_merge'] != 'both') |
        (merged['direction_csv'] != merged['direction_db']) |
        (~merged['confidence_csv'].eq(merged['confidence_db']))
    ].copy()

    # Report results
    if mismatches.empty:
        print("✔ All timestamps match between CSV and database from 2024-01-01 onwards.")
    else:
        print(f"✖ Found {len(mismatches)} mismatches from 2024-01-01 onwards. See details below:")
        print(mismatches[['timestamp', 'direction_csv', 'direction_db', 'confidence_csv', 'confidence_db', '_merge']])
        mismatches.to_csv(output_path, index=False)
        print(f"Mismatches saved to {output_path}")

if __name__ == '__main__':
    compare_predictions()
