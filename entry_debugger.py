# entry_debugger.py

import pandas as pd
from datetime import datetime, date, time as dt_time
from trade_config import TradeConfig
from db import get_conn, load_trade_state
from feature_generator import load_features
from predictor import load_predictions
from broker_utils import get_banknifty_futures_price, get_banknifty_spot_index_price, calculate_atm_strike

def debug_entry_pipeline():
    print("🔍 Entry Debugger — Start")
    now = datetime.now()
    now_time = now.time()

    # 1. Load predictions
    df_pred = load_predictions()
    if df_pred.empty:
        print("❌ No predictions found for today.")
        return

    print(f"✅ Loaded {len(df_pred)} prediction rows")

    # 2. Load features (optional)
    df_feat = load_features()

    # 3. Load state
    state = load_trade_state()
    print(f"📌 Current Trade State:")
    print(f"   - Date: {state.get('date')}")
    print(f"   - Active trades: {state.get('active_trade_count')}")
    print(f"   - Last trade #: {state.get('last_trade_number')}")

    # 4. Check entry window
    entry_start = dt_time.fromisoformat(TradeConfig.ENTRY_START)
    entry_end = dt_time.fromisoformat(TradeConfig.ENTRY_END)
    print(f"⏱️ Entry Window: {entry_start} - {entry_end} | Now: {now_time}")
    if not (entry_start <= now_time <= entry_end):
        print("❌ Not within entry time window")
        return

    # 5. Check trade caps
    if state["active_trade_count"] >= TradeConfig.MAX_CONCURRENT_TRADES:
        print("❌ Max concurrent trades reached")
        return
    if state["last_trade_number"] >= TradeConfig.MAX_TRADES_PER_DAY:
        print("❌ Max trades per day reached")
        return

    # 6. Market data
    spot = get_banknifty_spot_index_price()
    fut_ltp = get_banknifty_futures_price()
    if spot is None or fut_ltp is None:
        print("❌ Could not fetch spot/futures prices")
        return
    atm_strike = calculate_atm_strike(spot, TradeConfig.ATM_STRIKE_ROUNDING)
    print(f"📈 Market Data: Spot={spot}, Futures={fut_ltp}, ATM Strike={atm_strike}")

    # 7. Existing open trades
    with get_conn() as conn:
        rows = conn.execute("SELECT timestamp FROM live_trade_details WHERE status='OPEN'").fetchall()
        open_ts = {pd.to_datetime(r[0]) for r in rows}
    print(f"🧾 Open trade timestamps: {sorted(list(open_ts))[:3]}... (Total: {len(open_ts)})")

    # 8. Evaluate each prediction row
    print("\n--- Signal Analysis ---")
    for i, row in df_pred.iterrows():
        tsig = row["timestamp"]
        tsig_dt = pd.to_datetime(tsig)
        age = (now - tsig_dt).total_seconds()
        direction = row["direction"]
        raw_conf = row.get("confidence", 0.0)
        sm_long = row.get("entry_smoothed_long_conf", 0.0)
        sm_short = row.get("entry_smoothed_short_conf", 0.0)

        if tsig_dt in open_ts:
            print(f"[{tsig}] ⏩ Already open, skipping")
            continue
        if age > TradeConfig.ENTRY_MAX_SIGNAL_AGE:
            print(f"[{tsig}] ❌ Stale signal — Age = {age:.1f}s")
            continue

        if direction == "LONG":
            if sm_long >= TradeConfig.LONG_TH:
                print(f"[{tsig}] ✅ LONG passed: Smoothed={sm_long:.3f} ≥ TH={TradeConfig.LONG_TH}")
            else:
                print(f"[{tsig}] ❌ LONG failed: Smoothed={sm_long:.3f} < TH={TradeConfig.LONG_TH}")
        elif direction == "SHORT":
            if sm_short >= TradeConfig.SHORT_TH:
                print(f"[{tsig}] ✅ SHORT passed: Smoothed={sm_short:.3f} ≥ TH={TradeConfig.SHORT_TH}")
            else:
                print(f"[{tsig}] ❌ SHORT failed: Smoothed={sm_short:.3f} < TH={TradeConfig.SHORT_TH}")
        else:
            print(f"[{tsig}] ❌ Unknown direction: {direction}")

    print("🔍 Entry Debugger — End")

if __name__ == "__main__":
    debug_entry_pipeline()
