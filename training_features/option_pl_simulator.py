# features/option_pl_simulator.py

import pandas as pd

def simulate_option_pl(df, delta=0.5, lot_size=15, sl_points=30, r_multiplier=3):
    """
    Simulates expected P/L in ATM options using futures-based prediction targets.

    Adds:
        - option_profit_3R
        - option_loss_SL
        - expected_3R_profit  (+1 = TP hit, -1 = SL hit, 0 = neither)
        - r_multiple          (profit or loss in R units)

    Parameters:
        df (pd.DataFrame): Must contain 'max_profit_next_6'
        delta (float): ATM delta
        lot_size (int): Option lot size
        sl_points (float): SL in futures points
        r_multiplier (float): Target multiple (e.g., 3R)

    Returns:
        pd.DataFrame with P/L simulation columns
    """
    df = df.copy()

    target_fut = sl_points * r_multiplier  # e.g., 90 pts if 30 SL × 3R

    # Simulated option P/L
    df["option_profit_3R"] = target_fut * delta * lot_size
    df["option_loss_SL"] = -sl_points * delta * lot_size

    # Final outcome flag (+1 = 3R hit, -1 = SL hit, 0 = undecided)
    df["expected_3R_profit"] = df["max_profit_next_6"].apply(
        lambda x: 1 if x >= target_fut else (-1 if x <= -sl_points else 0)
    )

    # Net R multiple (realized result)
    df["r_multiple"] = df["expected_3R_profit"] * r_multiplier

    return df
