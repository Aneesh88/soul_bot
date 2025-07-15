# create_training_dataset.py

import sys
import os

# Add the current folder to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

# Now normal imports
from features.price_action import add_price_action_features
from features.volume_features import add_volume_features
from features.trend_features import add_trend_features
from features.structure_features import add_structure_features
from features.time_features import add_time_features
from features.momentum_features import add_momentum_features
from label_generator import generate_labels


# ==== CONFIGURATION ====
RAW_DATA_FILE = "/Users/aneeshvr/Desktop/BN-Raw Data/banknifty_1m_full_2012_2022.csv"  # <<< Full merged raw file
OUTPUT_FOLDER = "/Users/aneeshvr/Desktop/BN-Raw Data/output/"
OUTPUT_FILE = "dataset_with_labels.csv"

# Labeling parameters
SL_PERCENT = 0.002  # 0.2% stoploss assumption
FUTURE_WINDOW = 15  # 15 minute lookahead for future max high/min low

# ==== MAIN PIPELINE ====
def create_training_dataset():
    print("📥 Loading raw 1-minute BankNifty futures data...")
    df = pd.read_csv(RAW_DATA_FILE)

    print(f"🧠 Initial Data Loaded: {len(df)} rows, {df.shape[1]} columns")

    # ==== Feature Engineering ====
    print("🛠 Applying feature engineering modules...")

    df = add_price_action_features(df)
    print("✅ Price Action Features added.")

    df = add_volume_features(df)
    print("✅ Volume Features added.")

    df = add_trend_features(df)
    print("✅ Trend Features added.")

    df = add_structure_features(df)
    print("✅ Structure Features added.")

    df = add_time_features(df)
    print("✅ Time Features added.")

    df = add_momentum_features(df)
    print("✅ Momentum Features added.")

    # ==== Label Generation ====
    print("🏷️ Generating long and short labels based on future returns...")
    df = generate_labels(df, sl_percent=SL_PERCENT, future_window=FUTURE_WINDOW)
    print("✅ Labels added.")

    # ==== Save Output ====
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    output_path = os.path.join(OUTPUT_FOLDER, OUTPUT_FILE)

    df.to_csv(output_path, index=False)

    print(f"💾 Dataset saved to: {output_path}")
    print(f"🧠 Final Dataset: {len(df)} rows, {df.shape[1]} columns")
    print("🎯 Feature + Label Dataset creation completed successfully!")

# ==== RUN SCRIPT ====
if __name__ == "__main__":
    create_training_dataset()
