# features/meta_features.py

import numpy as np
import pandas as pd

def add_meta_quality_flags(df):
    """
    Adds high-confidence trade quality flags & meta features.
    Works in both strict and relaxed modes.
    """
    # === Volume spike confirmation
    df["volume_spike"] = df["volume"] / df["rolling_avg_vol_10"].replace(0, np.nan)

    # === Delta volume signal (simplified logic)
    df["delta_volume_sign"] = np.sign(df["volume_spike"] - 1)

    # === R/R ratio estimate (based on HVN levels)
    def compute_rr(row):
        if (
            pd.notna(row.get("distance_to_resistance")) and
            pd.notna(row.get("distance_to_support")) and
            row["distance_to_support"] != 0
        ):
            return round(row["distance_to_resistance"] / row["distance_to_support"], 2)
        return None

    df["rr_ratio_estimate"] = df.apply(compute_rr, axis=1)

    # === SR Cluster strength: count how many HVNs are close to current price
    def compute_zone_strength(row):
        close = row.get("close")
        zones = [row.get("dominant_hvn_above"), row.get("dominant_hvn_below"),
                 row.get("whvn_above"), row.get("whvn_below")]
        count = sum(
            1 for z in zones
            if pd.notna(z) and abs(z - close) <= 50  # within ₹50 proximity
        )
        return count

    df["zone_cluster_strength"] = df.apply(compute_zone_strength, axis=1)

    # === High confidence filter (relaxed logic)
    df["high_confidence_window"] = (
        (df["volume_spike"] > 1.5) &
        (df["oi_change"] > 0) &
        (df["rr_ratio_estimate"] > 2.0) &
        (df["zone_cluster_strength"] >= 2)
    ).astype(int)

    return df
