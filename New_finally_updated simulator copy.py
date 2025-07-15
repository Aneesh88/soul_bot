# New_finally_updated_simulator.py

import pandas as pd
import numpy as np
import os
from datetime import datetime, date, time, timedelta
from db import get_conn

# === FINALIZED PARAMETERS ===
LONG_TH                   = 0.85
SHORT_TH                  = 0.85
ENTRY_START               = "09:15"
ENTRY_END                 = "14:25"
FORCED_EXIT               = "15:13"
START_DATE                = "2025-07-01"
END_DATE                  = "2025-07-14"
FIXED_PROFIT_BOOKING      = 50
FIXED_STOP_LOSS           = 160
EXIT_SHORT_CONFIDENCE     = 0.51
EXIT_LONG_CONFIDENCE      = 0.51
MAX_CONCURRENT_TRADES     = 60
MAX_DAILY_TRADES          = 70

# === REPORT FOLDER ===
REPORT_FOLDER = "/Users/aneeshviswanathan/Desktop/Om_Babaji_Om_threshold"


# --- Helpers ---
def _parse_time(t_str):
    h, m = map(int, t_str.split(':'))
    return time(h, m)

def _parse_date(d_str):
    y, m, d = map(int, d_str.split('-'))
    return date(y, m, d)


# --- Data Loading ---
def load_data_from_db():
    """
    Load price bars, features, and all confidence series from new_predictions.
    """
    with get_conn() as conn:
        price_df = pd.read_sql(
            "SELECT * FROM bars ORDER BY timestamp", conn,
            parse_dates=["timestamp"]
        ).set_index("timestamp")

        feat_df = pd.read_sql(
            "SELECT * FROM features ORDER BY timestamp", conn,
            parse_dates=["timestamp"]
        ).set_index("timestamp")

        pred_df = pd.read_sql(
            """
            SELECT
              timestamp,
              direction,
              confidence,
              entry_smoothed_long_conf,
              entry_smoothed_short_conf,
              exit_smoothed_long_conf,
              exit_smoothed_short_conf
            FROM new_predictions
            ORDER BY timestamp
            """,
            conn,
            parse_dates=["timestamp"]
        ).set_index("timestamp")

    # Reconstruct raw “long_conf” / “short_conf” from direction + confidence
    pred_df["long_conf"]  = np.where(
        pred_df["direction"] == "LONG", pred_df["confidence"], 0.0
    )
    pred_df["short_conf"] = np.where(
        pred_df["direction"] == "SHORT", pred_df["confidence"], 0.0
    )

    return price_df, pred_df, feat_df


class Trade:
    def __init__(self, entry_time, direction, confidence, entry_price, sl, tp):
        self.entry_time   = entry_time
        self.direction    = direction
        self.confidence   = confidence
        self.entry_price  = entry_price
        self.sl           = sl
        self.tp           = tp
        self.exit_time    = None
        self.exit_price   = None
        self.pl           = None
        self.result       = None
        self.exit_reason  = None

    def close(self, exit_time, exit_price, reason=None):
        self.exit_time   = exit_time
        self.exit_price  = exit_price
        self.exit_reason = reason
        if self.direction == 'LONG':
            self.pl = exit_price - self.entry_price
        else:
            self.pl = self.entry_price - exit_price
        self.result = 'WIN' if self.pl > 0 else 'LOSS'


class Simulator:
    def __init__(self, price_df, pred_df, feat_df):
        self.price       = price_df['close']
        self.pred        = pred_df.copy()
        self.feat        = feat_df
        self.trades      = []
        self.open_trades = []

        self.entry_start = _parse_time(ENTRY_START)
        self.entry_end   = _parse_time(ENTRY_END)
        self.forced_exit = _parse_time(FORCED_EXIT)
        self.start_date  = _parse_date(START_DATE)
        self.end_date    = _parse_date(END_DATE)

    def run(self):
        current = self.start_date
        while current <= self.end_date:
            day_start = datetime.combine(current, time.min)
            day_end   = datetime.combine(current, time.max)
            prices    = self.price.loc[day_start:day_end]
            daily_trade_count = 0

            for ts, price_now in prices.items():
                ts_time = ts.time()

                # --- forced exit at end-of-day ---
                if ts_time >= self.forced_exit:
                    for t in list(self.open_trades):
                        t.close(ts, price_now, 'forced_exit')
                        self.trades.append(t)
                        self.open_trades.remove(t)

                # --- fixed TP/SL exits ---
                for t in list(self.open_trades):
                    if (t.direction == 'LONG' and price_now >= t.entry_price + FIXED_PROFIT_BOOKING) or \
                       (t.direction == 'SHORT' and price_now <= t.entry_price - FIXED_PROFIT_BOOKING):
                        t.close(ts, price_now, 'fixed_tp')

                    elif (t.direction == 'LONG' and price_now <= t.entry_price - FIXED_STOP_LOSS) or \
                         (t.direction == 'SHORT' and price_now >= t.entry_price + FIXED_STOP_LOSS):
                        t.close(ts, price_now, 'fixed_sl')

                    if t.exit_time:
                        self.trades.append(t)
                        self.open_trades.remove(t)

                # --- confidence-based exits ---
                if ts in self.pred.index:
                    exit_long_conf  = self.pred.at[ts, 'exit_smoothed_long_conf']
                    exit_short_conf = self.pred.at[ts, 'exit_smoothed_short_conf']
                    for t in list(self.open_trades):
                        if t.direction == 'LONG' and exit_short_conf >= EXIT_SHORT_CONFIDENCE:
                            t.close(ts, price_now, 'conf_short_exit')
                        elif t.direction == 'SHORT' and exit_long_conf >= EXIT_LONG_CONFIDENCE:
                            t.close(ts, price_now, 'conf_long_exit')

                        if t.exit_time:
                            self.trades.append(t)
                            self.open_trades.remove(t)

                # --- entry logic ---
                if (self.entry_start <= ts_time <= self.entry_end
                    and len(self.open_trades) < MAX_CONCURRENT_TRADES
                    and daily_trade_count < MAX_DAILY_TRADES):

                    if ts not in self.pred.index:
                        continue

                    entry_long_conf  = self.pred.at[ts, 'entry_smoothed_long_conf']
                    entry_short_conf = self.pred.at[ts, 'entry_smoothed_short_conf']

                    if entry_long_conf >= LONG_TH:
                        sl = price_now - FIXED_STOP_LOSS
                        tp = price_now + FIXED_PROFIT_BOOKING
                        self.open_trades.append(
                            Trade(ts, 'LONG', entry_long_conf, price_now, sl, tp)
                        )
                        daily_trade_count += 1

                    elif entry_short_conf >= SHORT_TH:
                        sl = price_now + FIXED_STOP_LOSS
                        tp = price_now - FIXED_PROFIT_BOOKING
                        self.open_trades.append(
                            Trade(ts, 'SHORT', entry_short_conf, price_now, sl, tp)
                        )
                        daily_trade_count += 1

            current += timedelta(days=1)

        # --- final close of any remaining open trades ---
        last_ts    = self.price.index[-1]
        last_price = self.price.iloc[-1]
        for t in self.open_trades:
            t.close(last_ts, last_price, 'final_close')
            self.trades.append(t)
        self.open_trades.clear()

    def results(self):
        # include exit_reason in the output
        return pd.DataFrame([{
            'entry_time' : t.entry_time,
            'exit_time'  : t.exit_time,
            'entry_price': t.entry_price,
            'exit_price' : t.exit_price,
            'exit_reason': t.exit_reason,
            'result'     : t.result,
            'pl'         : t.pl
        } for t in self.trades])


def save_report(trades_df: pd.DataFrame):
    trades_df['date']  = pd.to_datetime(trades_df['entry_time']).dt.date
    trades_df['week']  = pd.to_datetime(trades_df['entry_time']).dt.isocalendar().week
    trades_df['month'] = pd.to_datetime(trades_df['entry_time']).dt.to_period('M')

    daily = trades_df.groupby('date').agg(
        trades   = ('pl', 'count'),
        win_rate = ('result', lambda x: (x == 'WIN').mean()),
        total_pl = ('pl', 'sum')
    )
    weekly = trades_df.groupby('week').agg(
        trades   = ('pl', 'count'),
        win_rate = ('result', lambda x: (x == 'WIN').mean()),
        total_pl = ('pl', 'sum')
    )
    monthly = trades_df.groupby('month').agg(
        trades   = ('pl', 'count'),
        win_rate = ('result', lambda x: (x == 'WIN').mean()),
        total_pl = ('pl', 'sum')
    )

    summary = {
        'Period'      : f"{trades_df['entry_time'].min().date()} to {trades_df['exit_time'].max().date()}",
        'Total Trades': len(trades_df),
        'Win Rate'    : trades_df['result'].eq('WIN').mean(),
        'Total P/L'   : trades_df['pl'].sum()
    }

    lines = [
        "===== Trade Simulation Report =====",
        f"Period: {summary['Period']}",
        "",
        "===== Settings Used =====",
        f"LONG_TH               = {LONG_TH}",
        f"SHORT_TH              = {SHORT_TH}",
        f"MAX_CONCURRENT_TRADES = {MAX_CONCURRENT_TRADES}",
        f"MAX_DAILY_TRADES      = {MAX_DAILY_TRADES}",
        f"ENTRY_START           = {ENTRY_START}",
        f"ENTRY_END             = {ENTRY_END}",
        f"FORCED_EXIT           = {FORCED_EXIT}",
        f"FIXED_PROFIT_BOOKING  = {FIXED_PROFIT_BOOKING}",
        f"FIXED_STOP_LOSS       = {FIXED_STOP_LOSS}",
        f"EXIT_SHORT_CONFIDENCE = {EXIT_SHORT_CONFIDENCE}",
        f"EXIT_LONG_CONFIDENCE  = {EXIT_LONG_CONFIDENCE}",
        "",
        "===== Daily Report ====="
    ] + daily.to_string().splitlines() + [
        "",
        "===== Weekly Report ====="
    ] + weekly.to_string().splitlines() + [
        "",
        "===== Monthly Report ====="
    ] + monthly.to_string().splitlines() + [
        "",
        "===== Overall Summary ====="
    ] + [f"{k}: {v}" for k, v in summary.items()]

    # pick next available filename
    i = 1
    base = "simulation_report"
    while os.path.exists(os.path.join(REPORT_FOLDER, f"{base}_{i}.txt")):
        i += 1
    path = os.path.join(REPORT_FOLDER, f"{base}_{i}.txt")

    with open(path, 'w') as f:
        f.write("\n".join(lines))
    print(f"✅ Simulation report saved to: {path}")


def save_detailed_trades(trades_df: pd.DataFrame):
    """
    Write every trade to a detailed log including exit_reason.
    """
    lines = []
    for _, row in trades_df.iterrows():
        lines.append(
            f"Entry: {row['entry_time']}, Exit: {row['exit_time']}, "
            f"Entry Level: {row['entry_price']:.2f}, Exit Level: {row['exit_price']:.2f}, "
            f"Reason: {row['exit_reason']}, Result: {row['result']}, P/L: {row['pl']:.2f}"
        )

    # pick next detailed filename
    i = 1
    base = "detailed_trades"
    while os.path.exists(os.path.join(REPORT_FOLDER, f"{base}_{i}.txt")):
        i += 1
    path = os.path.join(REPORT_FOLDER, f"{base}_{i}.txt")

    with open(path, 'w') as f:
        f.write("\n".join(lines))
    print(f"✅ Detailed trades log saved to: {path}")


def main():
    price_df, pred_df, feat_df = load_data_from_db()
    sim = Simulator(price_df, pred_df, feat_df)
    sim.run()
    results_df = sim.results()
    save_report(results_df)
    save_detailed_trades(results_df)


if __name__ == '__main__':
    main()
