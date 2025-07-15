import sqlite3
import pandas as pd
from pathlib import Path

# Define paths
DB_PATH = Path(__file__).resolve().parent / "core_files" / "trading_data.db"
BARS_CSV = Path(__file__).resolve().parent / "BANKNIFTY_FUT_Oct2023_TO_Jul102025.csv"
FEATURES_CSV = Path(__file__).resolve().parent / "historical_features.csv"
PREDICTIONS_CSV = Path(__file__).resolve().parent / "historical_predictions.csv"

def restore_tables():
    print(f"🗂️  Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Clean tables
    print("🧹 Cleaning old data...")
    cur.execute("DELETE FROM bars")
    cur.execute("DELETE FROM features")
    cur.execute("DELETE FROM new_predictions")
    conn.commit()

    # Restore BARS table
    print("📥 Importing BARS from:", BARS_CSV)
    df_bars = pd.read_csv(BARS_CSV)
    df_bars.to_sql("bars", conn, if_exists="append", index=False)
    print(f"✅ Inserted {len(df_bars)} rows into bars")

    # Restore FEATURES table
    print("📥 Importing FEATURES from:", FEATURES_CSV)
    df_feat = pd.read_csv(FEATURES_CSV)
    df_feat.to_sql("features", conn, if_exists="append", index=False)
    print(f"✅ Inserted {len(df_feat)} rows into features")

    # Restore PREDICTIONS table
    print("📥 Importing PREDICTIONS from:", PREDICTIONS_CSV)
    df_preds = pd.read_csv(PREDICTIONS_CSV)
    df_preds.to_sql("new_predictions", conn, if_exists="append", index=False)
    print(f"✅ Inserted {len(df_preds)} rows into new_predictions")

    conn.close()
    print("🎉 Database restore complete.")

if __name__ == "__main__":
    restore_tables()
