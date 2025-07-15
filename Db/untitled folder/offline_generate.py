#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

# import your real logic from the existing modules
from feature_generator import _compute_ema_filter_15, build_features
from predictor import load_model, get_feature_order, prepare_features_for_prediction, MODEL_PATH
from trade_config import TradeConfig

CANDLE_CSV      = "bars_2025-06-19.csv"
FEATURES_CSV    = f"features_{CANDLE_CSV.split('_')[1]}"
PREDICTIONS_CSV = f"predictions_{CANDLE_CSV.split('_')[1]}"

def offline_generate():
    # 1) Load today's candle data
    df_bars = pd.read_csv(CANDLE_CSV, parse_dates=["timestamp"])
    # ensure timestamp string column
    df_bars["timestamp"] = df_bars["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # 2) Compute the EMA-regime filter exactly as in feature_generator.py
    df_bars["ema_filter_15"] = _compute_ema_filter_15(df_bars)

    # 3) Build all other features using your exact pipeline
    df_feat = build_features(df_bars)

    # 4) Normalize timestamp/date columns for CSV output
    df_feat["timestamp"] = pd.to_datetime(df_feat["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    if "datetime" in df_feat.columns:
        df_feat["datetime"] = pd.to_datetime(df_feat["datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    if "date" in df_feat.columns:
        df_feat["date"] = df_feat["date"].astype(str)

    # 5) Write out the features CSV
    df_feat.to_csv(FEATURES_CSV, index=False)
    print(f"✅ Wrote {len(df_feat)} feature rows → {FEATURES_CSV}")

    # 6) Load your trained model from the same location predictor.py uses
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Model not found at {MODEL_PATH}")
    model = load_model(MODEL_PATH)

    # 7) Align the feature‐DataFrame to the model’s expected columns
    feature_order = get_feature_order(model)
    X = prepare_features_for_prediction(df_feat, feature_order)

    # 8) Run predict_proba and build the predictions DataFrame
    proba   = model.predict_proba(X)
    dirs    = ["LONG" if p[1] >= p[0] else "SHORT" for p in proba]
    confs   = [float(round(max(p), 4)) for p in proba]
    df_pred = pd.DataFrame({
        "timestamp": df_feat["timestamp"],
        "direction": dirs,
        "confidence": confs
    })

    # 9) Write out the predictions CSV
    df_pred.to_csv(PREDICTIONS_CSV, index=False)
    print(f"✅ Wrote {len(df_pred)} prediction rows → {PREDICTIONS_CSV}")

if __name__ == "__main__":
    offline_generate()
