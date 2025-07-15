# temp_feature_gen.py
"""
Append-only test for new features (no EMA), guaranteed not to overwrite your existing EVAL_features_final.csv
"""

import os
import pandas as pd
from pathlib import Path

# Absolute paths
CORE_CSV = Path("/Users/aneeshviswanathan/Desktop/om_babaji_om/core_files/JAN2024_TO_JUN2025_BANKNIFTY_FUT.csv")
FEAT_CSV = Path("/Users/aneeshviswanathan/Desktop/om_babaji_om/core_files/EVAL_features_final.csv")

# Feature‐engineering imports (as before) …
from training_features.price_action       import add_price_action_features
from training_features.volume_features    import add_volume_features
from training_features.volume_signals     import add_volume_features as add_volume_signals
from training_features.open_interest      import add_oi_features as add_open_interest_features
from training_features.structure_features import add_structure_features
from training_features.vwap_utils         import calculate_vwap as add_vwap_features
from training_features.hvn_engine         import compute_rolling_hvn as add_hvn_features
from training_features.time_features      import add_time_features
from training_features.momentum_features  import add_momentum_features
from training_features.trend_detector     import add_trend_regime as add_trend_label
from training_features.meta_features      import add_meta_quality_flags
from training_features.features_engineered import add_feature_engineering

def build_features_no_ema(df: pd.DataFrame) -> pd.DataFrame:
    # (same pipeline as before)
    if 'oi' in df.columns and 'open_interest' not in df.columns:
        df['open_interest'] = df['oi']
    elif 'open_interest' in df.columns and 'oi' not in df.columns:
        df['oi'] = df['open_interest']

    df['datetime'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.sort_values('datetime').reset_index(drop=True)

    df['prev_close'] = df['close'].shift(1)
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = (df['high'] - df['prev_close']).abs()
    df['tr3'] = (df['low'] - df['prev_close']).abs()
    df['true_range'] = df[['tr1','tr2','tr3']].max(axis=1)
    df['atr'] = df['true_range'].rolling(14, min_periods=1).mean()
    df.drop(columns=['prev_close','tr1','tr2','tr3','true_range'], inplace=True)

    df = add_price_action_features(df)
    df = add_volume_features(df)
    df = add_volume_signals(df)
    df = add_open_interest_features(df)
    df = add_structure_features(df)
    df = add_vwap_features(df)
    df = add_hvn_features(df)
    df = add_time_features(df)
    df = add_momentum_features(df)
    df = add_trend_label(df)
    df = add_meta_quality_flags(df)

    df = add_feature_engineering(df, str(FEAT_CSV))
    return df

def main():
    # Check file sizes before
    if FEAT_CSV.exists():
        before_size = FEAT_CSV.stat().st_size
        print(f"Features CSV before: {FEAT_CSV} ({before_size} bytes)")
    else:
        print(f"Features CSV does not exist at {FEAT_CSV}")

    # 1) Load raw bars
    df_bars = pd.read_csv(CORE_CSV)
    df_bars['timestamp'] = pd.to_datetime(df_bars['timestamp'], errors='coerce')
    df_bars.sort_values('timestamp', inplace=True)

    # 2) Determine last feature timestamp
    if FEAT_CSV.exists():
        df_prev = pd.read_csv(FEAT_CSV)
        df_prev['timestamp'] = pd.to_datetime(df_prev['timestamp'], errors='coerce')
        last_ts = df_prev['timestamp'].max()
    else:
        last_ts = df_bars['timestamp'].min() - pd.Timedelta(minutes=1)

    df_new = df_bars[df_bars['timestamp'] > last_ts].copy()
    print(f"New bars to process: {len(df_new)}")

    if df_new.empty:
        print("No new bars; nothing to append.")
    else:
        df_feat = build_features_no_ema(df_new)
        FEAT_CSV.parent.mkdir(parents=True, exist_ok=True)
        # Always append, never write header
        df_feat.to_csv(FEAT_CSV, mode='a', header=False, index=False)
        print(f"Appended {len(df_feat)} rows to {FEAT_CSV}")

    # Check file size after
    if FEAT_CSV.exists():
        after_size = FEAT_CSV.stat().st_size
        print(f"Features CSV after:  {FEAT_CSV} ({after_size} bytes)")
        print(f"Size increased by:    {after_size - before_size} bytes")

if __name__ == "__main__":
    main()
