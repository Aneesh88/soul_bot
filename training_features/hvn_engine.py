# features/hvn_engine.py

import pandas as pd
from collections import defaultdict

def compute_rolling_hvn(df, window_size=1500):
    """
    Computes high-volume nodes and distances from rolling price-volume data.
    Outputs:
        - dominant_hvn_above / below
        - whvn_above / below
        - distance_to_resistance
        - distance_to_support
    """
    dominant_above = []
    dominant_below = []
    weighted_above = []
    weighted_below = []
    dist_res = []
    dist_sup = []

    for idx in range(len(df)):
        if idx < window_size:
            # Not enough data to compute HVNs
            dominant_above.append(None)
            dominant_below.append(None)
            weighted_above.append(None)
            weighted_below.append(None)
            dist_res.append(None)
            dist_sup.append(None)
            continue

        window = df.iloc[idx - window_size:idx]
        close = df.iloc[idx]["close"]

        # Price-volume map rounded to nearest ₹1
        price_volume = defaultdict(float)
        for _, row in window.iterrows():
            price = round(row["close"])
            price_volume[price] += row["volume"]

        # Sort by volume
        sorted_hvns = sorted(price_volume.items(), key=lambda x: x[1], reverse=True)

        # Separate above/below HVNs
        hvn_above = [(p, v) for p, v in sorted_hvns if p > close]
        hvn_below = [(p, v) for p, v in sorted_hvns if p < close]

        # Dominant HVN (highest volume)
        dom_above = hvn_above[0][0] if hvn_above else None
        dom_below = hvn_below[0][0] if hvn_below else None

        # Weighted HVN
        def weighted(hvns):
            total_vol = sum(v for _, v in hvns)
            return round(sum(p * v for p, v in hvns) / total_vol, 2) if total_vol > 0 else None

        whvn_abv = weighted(hvn_above)
        whvn_blw = weighted(hvn_below)

        # Distances
        dist_r = dom_above - close if dom_above is not None else None
        dist_s = close - dom_below if dom_below is not None else None

        # Append all
        dominant_above.append(dom_above)
        dominant_below.append(dom_below)
        weighted_above.append(whvn_abv)
        weighted_below.append(whvn_blw)
        dist_res.append(dist_r)
        dist_sup.append(dist_s)

    df["dominant_hvn_above"] = dominant_above
    df["dominant_hvn_below"] = dominant_below
    df["whvn_above"] = weighted_above
    df["whvn_below"] = weighted_below
    df["distance_to_resistance"] = dist_res
    df["distance_to_support"] = dist_sup

    return df
