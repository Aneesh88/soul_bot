"""
export_trade_tables.py

Standalone script to export the two DB-backed trade tables to CSV files in a "state" subfolder
relative to the script's directory:
 - live_trade_details → state/live_trade_details.csv
 - daily_trade_state → state/daily_trade_state.csv
"""
import os
import pandas as pd
from pathlib import Path
from db import get_conn, init_db

# Ensure DB schema exists
init_db()

# Directory for exports: next to this script
SCRIPT_DIR = Path(__file__).resolve().parent
STATE_DIR = SCRIPT_DIR / "state"
STATE_DIR.mkdir(exist_ok=True)

# Export live_trade_details
def export_live_trades(output_dir: Path = STATE_DIR):
    """Exports the live_trade_details table to a CSV file in the given directory."""
    output_path = output_dir / "live_trade_details.csv"
    with get_conn() as conn:
        df = pd.read_sql("SELECT * FROM live_trade_details", conn)
    df.to_csv(output_path, index=False)
    print(f"✅ Exported live_trade_details to {output_path}")

# Export daily_trade_state
def export_daily_state(output_dir: Path = STATE_DIR):
    """Exports the daily_trade_state table to a CSV file in the given directory."""
    output_path = output_dir / "daily_trade_state.csv"
    with get_conn() as conn:
        df = pd.read_sql("SELECT * FROM daily_trade_state", conn)
    df.to_csv(output_path, index=False)
    print(f"✅ Exported daily_trade_state to {output_path}")

if __name__ == '__main__':
    export_live_trades()
    export_daily_state()
