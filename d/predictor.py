import logging
import pickle
import sqlite3
from pathlib import Path
import warnings

import pandas as pd
import numpy as np
from trade_config import TradeConfig
from db import init_db, get_conn

warnings.filterwarnings(
    "ignore",
    message="Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated"
)

log = logging.getLogger(__name__)
console = logging.getLogger("console")
MODEL_PATH = Path(__file__).resolve().parent / TradeConfig.MODEL_PKL

def load_model(path: Path):
    with open(path, "rb") as f:
        return pickle.load(f)

def get_feature_order(model) -> list[str]:
    if hasattr(model, "feature_names_in_"):
        return list(model.feature_names_in_)
    if hasattr(model, "get_booster"):
        return model.get_booster().feature_names
    raise ValueError("Could not determine feature order from model")

def prepare_features_for_prediction(df: pd.DataFrame, feature_order: list[str]) -> pd.DataFrame:
    df2 = df.copy()
    for col in feature_order:
        if col not in df2.columns:
            df2[col] = 0
    return df2[feature_order].fillna(0)

def predictor_cycle() -> int:
    init_db()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT MAX(timestamp) FROM new_predictions")
        row = cur.fetchone()
        last_ts = row[0] if row and row[0] is not None else None

    if last_ts is None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT MAX(timestamp) FROM features")
            seed_row = cur.fetchone()
            seed = seed_row[0] if seed_row and seed_row[0] else None
            if seed:
                cur.execute(
                    "INSERT OR IGNORE INTO new_predictions(timestamp, direction, confidence, long_conf, short_conf) VALUES (?, ?, ?, ?, ?)",
                    (seed, "LONG", 1.0, 1.0, 0.0)
                )
                conn.commit()
                log.info("Seeded new_predictions at %s", seed)
        return 0

    with get_conn() as conn:
        df_feat = pd.read_sql(
            "SELECT * FROM features WHERE timestamp > ? ORDER BY timestamp", conn,
            params=(last_ts,), parse_dates=["timestamp"]
        )

    if df_feat.empty:
        log.info("No new feature rows to predict on.")
        return 0

    if not MODEL_PATH.exists():
        log.error("Model file not found: %s", MODEL_PATH)
        return 0

    model = load_model(MODEL_PATH)
    feature_order = get_feature_order(model)
    X = prepare_features_for_prediction(df_feat, feature_order)
    proba = model.predict_proba(X)

    long_conf = [float(round(p[1], 4)) for p in proba]
    short_conf = [float(round(p[0], 4)) for p in proba]

    directions = ["LONG" if lc >= sc else "SHORT" for lc, sc in zip(long_conf, short_conf)]
    confidences = [max(lc, sc) for lc, sc in zip(long_conf, short_conf)]

    insert_sql = (
        "INSERT OR IGNORE INTO new_predictions "
        "(timestamp, direction, confidence, long_conf, short_conf) "
        "VALUES (?, ?, ?, ?, ?)"
    )

    inserted = 0
    with get_conn() as conn:
        cur = conn.cursor()
        for i, ts in enumerate(df_feat["timestamp"]):
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
            try:
                cur.execute(insert_sql, (
                    ts_str, directions[i], confidences[i],
                    long_conf[i], short_conf[i]
                ))
                if cur.rowcount:
                    ts_short = ts_str[:16]
                    emoji = "🐂" if directions[i] == "LONG" else "🐻"
                    console.info(
                        f"⏱️ [{ts_short}] 📈 {directions[i]:<5} (Conf: {confidences[i]:.2f}) | Model Prediction: {emoji}"
                    )
                inserted += cur.rowcount
            except sqlite3.DatabaseError as e:
                log.error("DB insert error at %s: %s", ts_str, e)
        conn.commit()

    log.info("Inserted %d new predictions into 'new_predictions' table", inserted)
    return inserted

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    count = predictor_cycle()
    console.info(f"✅ Inserted {count} new predictions")
