# migrate_features_csv.py
"""
Add the missing 'ema_filter_15' column to your historical features CSV,
filling it with False (or NaN) so that your new rows line up perfectly.
"""

import pandas as pd
from pathlib import Path

# 1) Point to your existing features CSV
FEAT_CSV = Path("/Users/aneeshviswanathan/Desktop/om_babaji_om/core_files/EVAL_features_final.csv")

# 2) Load it
df = pd.read_csv(FEAT_CSV)

# 3) If the column is already present, do nothing
if 'ema_filter_15' not in df.columns:
    # 4) Insert the column right after 'date' (or wherever you like)
    insert_at = df.columns.get_loc('date') + 1
    df.insert(insert_at, 'ema_filter_15', False)  # or use pd.NA / np.nan

    # 5) Overwrite the CSV with the new header
    df.to_csv(FEAT_CSV, index=False)
    print(f"✅ Added 'ema_filter_15' column to {FEAT_CSV}")
else:
    print(f"'ema_filter_15' already present in {FEAT_CSV}")
