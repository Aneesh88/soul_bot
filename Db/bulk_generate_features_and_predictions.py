import pandas as pd
import logging
from pathlib import Path

from feature_generator import build_features
from predictor import load_model, get_feature_order, prepare_features_for_prediction, weighted_smoothing
from trade_config import TradeConfig

# Setup
logging.basicConfig(level=logging.INFO)
MODEL_PATH = Path(__file__).resolve().parent / TradeConfig.MODEL_PKL

# === Step 1: Load Cleaned Bars ===
BARS_CSV = Path("/Users/aneeshviswanathan/Desktop/Om_Babaji_Om_threshold/BANKNIFTY_FUT_Oct2023_TO_Jul102025.csv")
df_bars = pd.read_csv(BARS_CSV)
df_bars["timestamp"] = pd.to_datetime(df_bars["timestamp"])
df_bars["ema_filter_15"] = 1  # force 1 to proceed with offline generation

# === Step 2: Generate Features ===
df_feat = build_features(df_bars)
df_feat.to_csv("historical_features.csv", index=False)
print("✅ Features saved to historical_features.csv")

# === Step 3: Generate Predictions ===
if not MODEL_PATH.exists():
    raise FileNotFoundError(f"❌ Model not found at {MODEL_PATH}")

model = load_model(MODEL_PATH)
feature_order = get_feature_order(model)
X = prepare_features_for_prediction(df_feat, feature_order)
proba = model.predict_proba(X)

# Extract confidence scores
long_conf = [float(round(p[1], 4)) for p in proba]
short_conf = [float(round(p[0], 4)) for p in proba]

# Apply smoothing
long_series = pd.Series(long_conf)
short_series = pd.Series(short_conf)

entry_smoothed_long = weighted_smoothing(long_series, TradeConfig.ENTRY_SMOOTHING_WINDOW)
entry_smoothed_short = weighted_smoothing(short_series, TradeConfig.ENTRY_SMOOTHING_WINDOW)
exit_smoothed_long = weighted_smoothing(long_series, TradeConfig.EXIT_SMOOTHING_WINDOW)
exit_smoothed_short = weighted_smoothing(short_series, TradeConfig.EXIT_SMOOTHING_WINDOW)

# Decide direction
directions = ["LONG" if lc >= sc else "SHORT" for lc, sc in zip(long_conf, short_conf)]
confidences = [max(lc, sc) for lc, sc in zip(long_conf, short_conf)]

# Prepare final DataFrame
df_preds = pd.DataFrame({
    "timestamp": df_feat["timestamp"],
    "direction": directions,
    "confidence": confidences,
    "long_conf": long_conf,
    "short_conf": short_conf,
    "entry_smoothed_long_conf": entry_smoothed_long,
    "entry_smoothed_short_conf": entry_smoothed_short,
    "exit_smoothed_long_conf": exit_smoothed_long,
    "exit_smoothed_short_conf": exit_smoothed_short,
})
df_preds.to_csv("historical_predictions.csv", index=False)
print("✅ Predictions saved to historical_predictions.csv")
