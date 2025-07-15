import pandas as pd
from pathlib import Path

# File paths
bars_path = Path("BANKNIFTY_FUT_Oct2023_TO_Jul102025.csv")
features_path = Path("historical_features.csv")
preds_path = Path("historical_predictions.csv")
report_path = Path("state/csv_integrity_report.txt")

# Ensure 'state' folder exists
report_path.parent.mkdir(parents=True, exist_ok=True)

# Load data
df_bars = pd.read_csv(bars_path)
df_feat = pd.read_csv(features_path)
df_pred = pd.read_csv(preds_path)

# Convert timestamps
df_bars["timestamp"] = pd.to_datetime(df_bars["timestamp"])
df_feat["timestamp"] = pd.to_datetime(df_feat["timestamp"])
df_pred["timestamp"] = pd.to_datetime(df_pred["timestamp"])

lines = []

def check_basic_alignment():
    lines.append("📊 CSV INTEGRITY REPORT")
    lines.append("-" * 50)

    # Row count check
    lines.append(f"✅ Rows in Bars: {len(df_bars)}")
    lines.append(f"✅ Rows in Features: {len(df_feat)}")
    lines.append(f"✅ Rows in Predictions: {len(df_pred)}")
    if not (len(df_bars) == len(df_feat) == len(df_pred)):
        lines.append("❌ ERROR: Row counts are mismatched!")

    # Timestamp match
    ts_bars = df_bars["timestamp"]
    ts_feat = df_feat["timestamp"]
    ts_pred = df_pred["timestamp"]

    if not ts_bars.equals(ts_feat):
        lines.append("❌ ERROR: Bars and Features timestamps mismatch")
    if not ts_feat.equals(ts_pred):
        lines.append("❌ ERROR: Features and Predictions timestamps mismatch")
    if ts_bars.equals(ts_feat) and ts_feat.equals(ts_pred):
        lines.append("✅ Timestamps aligned across all 3 files")

    # Chronological check
    if not ts_bars.is_monotonic_increasing:
        lines.append("❌ ERROR: Bars timestamps not in chronological order")
    else:
        lines.append("✅ Bars timestamps are chronologically ordered")

    # Null checks
    for df, name in [(df_bars, "Bars"), (df_feat, "Features"), (df_pred, "Predictions")]:
        null_count = df.isnull().sum().sum()
        if null_count > 0:
            lines.append(f"❌ ERROR: {null_count} null values found in {name}")
        else:
            lines.append(f"✅ No nulls in {name}")

    # Duplicate timestamps
    for df, name in [(df_bars, "Bars"), (df_feat, "Features"), (df_pred, "Predictions")]:
        if df["timestamp"].duplicated().any():
            dupes = df[df["timestamp"].duplicated()]
            lines.append(f"❌ ERROR: {len(dupes)} duplicate timestamps in {name}")
        else:
            lines.append(f"✅ No duplicate timestamps in {name}")

def check_feature_nulls():
    null_summary = df_feat.isnull().sum()
    null_columns = null_summary[null_summary > 0]

    if null_columns.empty:
        return

    lines.append("\n🕵️‍♂️ NULL VALUE ANALYSIS IN FEATURES")
    lines.append("-" * 50)

    for col in null_columns.index:
        null_rows = df_feat[df_feat[col].isnull()]
        count = len(null_rows)
        first_ts = null_rows["timestamp"].min()
        last_ts = null_rows["timestamp"].max()
        lines.append(f"🔸 {col}: {count} nulls from {first_ts} to {last_ts}")

def write_report():
    report_path.write_text("\n".join(lines))
    print("\n".join(lines))
    print(f"\n📁 Report saved to: {report_path}")

# Run checks
check_basic_alignment()
check_feature_nulls()
write_report()
