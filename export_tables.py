# export_tables.py
"""
Export all three SQLite tables ('bars', 'features', 'new_predictions')
to CSV files in the core_files directory, using the paths defined
in TradeConfig so you overwrite your existing CSVs.
"""

import pandas as pd
from pathlib import Path
from db import get_conn
from trade_config import TradeConfig

def export_table(table_name: str, output_path: Path):
    """
    Read the entire table from SQLite and write it to CSV.
    """
    with get_conn() as conn:
        df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY timestamp", conn)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Exported {len(df)} rows from '{table_name}' → {output_path}")

def main():
    # Export using the updated table names and CSV paths
    mapping = {
        "bars":             Path(TradeConfig.CORE_CSV),
        "features":         Path(TradeConfig.FEATURES_CSV),
        "new_predictions":  Path(TradeConfig.PRED_CSV),  # ✅ Updated from 'predictions'
    }

    for table, path in mapping.items():
        export_table(table, path)

if __name__ == "__main__":
    main()
