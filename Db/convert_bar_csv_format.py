import pandas as pd
from pathlib import Path

# Absolute path to your downloaded file
csv_path = Path("/Users/aneeshviswanathan/Desktop/Om_Babaji_Om_threshold/BANKNIFTY_FUT_Oct2023_TO_Jul102025.csv")

# Load the CSV
df = pd.read_csv(csv_path)

# Rename 'oi' → 'open_interest'
df.rename(columns={"oi": "open_interest"}, inplace=True)

# Convert ISO timestamp to desired format: "YYYY-MM-DD HH:MM:SS"
df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")

# Overwrite the same file with cleaned data
df.to_csv(csv_path, index=False)

print(f"✅ Cleaned and overwritten: {csv_path}")
