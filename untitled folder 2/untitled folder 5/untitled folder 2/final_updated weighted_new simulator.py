import pandas as pd
import numpy as np
import os
from datetime import datetime, date, time, timedelta

# === Finalized Settings ===
LONG_TH = 0.85
SHORT_TH = 0.85
MAX_CONCURRENT_TRADES = 60
MAX_DAILY_TRADES = 70
ENTRY_START = "09:15"
ENTRY_END = "14:25"
FORCED_EXIT = "15:13"
START_DATE = "2025-07-01"
END_DATE = "2025-07-09"
FIXED_PROFIT_BOOKING = 50
FIXED_STOP_LOSS = 160

ENABLE_CONFIDENCE_BASED_EXIT = True
EXIT_SHORT_CONFIDENCE = 0.51
EXIT_LONG_CONFIDENCE = 0.51

ENABLE_ENTRY_SMOOTHING = True
ENTRY_SMOOTHING_WINDOW = 3
WEIGHTED_ENTRY_SMOOTHING = True

ENABLE_EXIT_SMOOTHING = True
EXIT_SMOOTHING_WINDOW = 15
WEIGHTED_EXIT_SMOOTHING = True

# === File Paths ===
PRICE_CSV = "/Users/aneeshviswanathan/Desktop/Om_Babaji_Om_threshold/core_files/JAN2024_TO_JUN2025_BANKNIFTY_FUT.csv"
PRED_CSV = "/Users/aneeshviswanathan/Desktop/Om_Babaji_Om_threshold/core_files/model_predictions.csv"
FEAT_CSV = "/Users/aneeshviswanathan/Desktop/Om_Babaji_Om_threshold/core_files/EVAL_features_final.csv"

def _parse_time(t): return time(*map(int, t.split(":")))
def _parse_date(d): return date(*map(int, d.split("-")))

def get_weighted_rolling(series, window):
    weights = np.arange(1, window + 1)
    weights = weights / weights.sum()
    return series.rolling(window).apply(lambda x: np.dot(x, weights[-len(x):]), raw=True)

class Trade:
    def __init__(self, entry_time, direction, confidence, entry_price, sl, tp):
        self.entry_time = entry_time
        self.direction = direction
        self.confidence = confidence
        self.entry_price = entry_price
        self.sl = sl
        self.tp = tp
        self.exit_time = None
        self.exit_price = None
        self.pl = None
        self.result = None
        self.exit_reason = None

    def close(self, exit_time, exit_price, reason=None):
        self.exit_time = exit_time
        self.exit_price = exit_price
        self.exit_reason = reason
        self.pl = exit_price - self.entry_price if self.direction == 'LONG' else self.entry_price - exit_price
        self.result = 'WIN' if self.pl > 0 else 'LOSS'

class Simulator:
    def __init__(self, price_df, pred_df, feat_df):
        self.price = price_df['close']
        self.pred = pred_df.copy()
        self.feat = feat_df
        self.trades, self.open_trades = [], []
        self.entry_start = _parse_time(ENTRY_START)
        self.entry_end = _parse_time(ENTRY_END)
        self.forced_exit = _parse_time(FORCED_EXIT)
        self.start_date = _parse_date(START_DATE)
        self.end_date = _parse_date(END_DATE)

        if ENABLE_ENTRY_SMOOTHING:
            if WEIGHTED_ENTRY_SMOOTHING:
                self.pred['smoothed_long_conf'] = get_weighted_rolling(self.pred['long_conf'], ENTRY_SMOOTHING_WINDOW)
                self.pred['smoothed_short_conf'] = get_weighted_rolling(self.pred['short_conf'], ENTRY_SMOOTHING_WINDOW)
            else:
                self.pred['smoothed_long_conf'] = self.pred['long_conf'].rolling(ENTRY_SMOOTHING_WINDOW).mean()
                self.pred['smoothed_short_conf'] = self.pred['short_conf'].rolling(ENTRY_SMOOTHING_WINDOW).mean()
        else:
            self.pred['smoothed_long_conf'] = self.pred['long_conf']
            self.pred['smoothed_short_conf'] = self.pred['short_conf']

        if ENABLE_EXIT_SMOOTHING:
            if WEIGHTED_EXIT_SMOOTHING:
                self.pred['exit_smoothed_long_conf'] = get_weighted_rolling(self.pred['long_conf'], EXIT_SMOOTHING_WINDOW)
                self.pred['exit_smoothed_short_conf'] = get_weighted_rolling(self.pred['short_conf'], EXIT_SMOOTHING_WINDOW)
            else:
                self.pred['exit_smoothed_long_conf'] = self.pred['long_conf'].rolling(EXIT_SMOOTHING_WINDOW).mean()
                self.pred['exit_smoothed_short_conf'] = self.pred['short_conf'].rolling(EXIT_SMOOTHING_WINDOW).mean()
        else:
            self.pred['exit_smoothed_long_conf'] = self.pred['long_conf']
            self.pred['exit_smoothed_short_conf'] = self.pred['short_conf']

    def run(self):
        current = self.start_date
        while current <= self.end_date:
            daily_trade_count = 0
            prices = self.price.loc[datetime.combine(current, time.min):datetime.combine(current, time.max)]

            for ts, price_now in prices.items():
                ts_time = ts.time()

                if ts_time >= self.forced_exit:
                    for t in list(self.open_trades):
                        t.close(ts, price_now, 'forced_exit')
                        self.trades.append(t)
                        self.open_trades.remove(t)

                for t in list(self.open_trades):
                    if (t.direction == 'LONG' and price_now >= t.tp) or (t.direction == 'SHORT' and price_now <= t.tp):
                        t.close(ts, price_now, 'tp_hit')
                    elif (t.direction == 'LONG' and price_now <= t.sl) or (t.direction == 'SHORT' and price_now >= t.sl):
                        t.close(ts, price_now, 'sl_hit')
                    elif (price_now >= t.entry_price + FIXED_PROFIT_BOOKING and t.direction == 'LONG') or \
                         (price_now <= t.entry_price - FIXED_PROFIT_BOOKING and t.direction == 'SHORT'):
                        t.close(ts, price_now, 'fixed_tp')
                    elif (price_now <= t.entry_price - FIXED_STOP_LOSS and t.direction == 'LONG') or \
                         (price_now >= t.entry_price + FIXED_STOP_LOSS and t.direction == 'SHORT'):
                        t.close(ts, price_now, 'fixed_sl')
                    if t.exit_time:
                        self.trades.append(t)
                        self.open_trades.remove(t)

                if ENABLE_CONFIDENCE_BASED_EXIT and ts in self.pred.index:
                    for t in list(self.open_trades):
                        long_conf = self.pred.at[ts, 'exit_smoothed_long_conf']
                        short_conf = self.pred.at[ts, 'exit_smoothed_short_conf']
                        if t.direction == 'LONG' and short_conf >= EXIT_SHORT_CONFIDENCE:
                            t.close(ts, price_now, 'conf_short_exit')
                        elif t.direction == 'SHORT' and long_conf >= EXIT_LONG_CONFIDENCE:
                            t.close(ts, price_now, 'conf_long_exit')
                        if t.exit_time:
                            self.trades.append(t)
                            self.open_trades.remove(t)

                if self.entry_start <= ts_time <= self.entry_end and len(self.open_trades) < MAX_CONCURRENT_TRADES:
                    if ts not in self.pred.index or ts not in self.feat.index:
                        continue
                    if daily_trade_count >= MAX_DAILY_TRADES:
                        continue
                    long_conf = self.pred.at[ts, 'smoothed_long_conf']
                    short_conf = self.pred.at[ts, 'smoothed_short_conf']
                    atr = self.feat.at[ts, 'atr']
                    if long_conf >= LONG_TH:
                        self.open_trades.append(Trade(ts, 'LONG', long_conf, price_now, price_now - 100 * atr, price_now + 300 * atr))
                        daily_trade_count += 1
                    elif short_conf >= SHORT_TH:
                        self.open_trades.append(Trade(ts, 'SHORT', short_conf, price_now, price_now + 100 * atr, price_now - 300 * atr))
                        daily_trade_count += 1
            current += timedelta(days=1)

        for t in self.open_trades:
            t.close(self.price.index[-1], self.price.iloc[-1], 'final_close')
            self.trades.append(t)

    def results(self):
        return pd.DataFrame([vars(t) for t in self.trades])

def save_report(trades_df):
    trades_df['date'] = pd.to_datetime(trades_df['entry_time']).dt.date
    summary = {
        'Period': f"{trades_df['entry_time'].min().date()} to {trades_df['exit_time'].max().date()}",
        'Total Trades': len(trades_df),
        'Win Rate': trades_df['result'].eq('WIN').mean(),
        'Total P/L': trades_df['pl'].sum()
    }

    output_lines = [
        "===== Final Simulator Report =====",
        f"Period: {summary['Period']}",
        "--- Settings ---",
        f"LONG_TH = {LONG_TH}",
        f"SHORT_TH = {SHORT_TH}",
        f"MAX_CONCURRENT_TRADES = {MAX_CONCURRENT_TRADES}",
        f"MAX_DAILY_TRADES = {MAX_DAILY_TRADES}",
        f"ENTRY_START = {ENTRY_START}",
        f"ENTRY_END = {ENTRY_END}",
        f"FORCED_EXIT = {FORCED_EXIT}",
        f"FIXED_PROFIT_BOOKING = {FIXED_PROFIT_BOOKING}",
        f"FIXED_STOP_LOSS = {FIXED_STOP_LOSS}",
        f"EXIT_SHORT_CONFIDENCE = {EXIT_SHORT_CONFIDENCE}",
        f"EXIT_LONG_CONFIDENCE = {EXIT_LONG_CONFIDENCE}",
        f"ENTRY_SMOOTHING_WINDOW = {ENTRY_SMOOTHING_WINDOW} (Weighted)",
        f"EXIT_SMOOTHING_WINDOW = {EXIT_SMOOTHING_WINDOW} (Weighted)",
        "===== Overall Summary =====",
    ] + [f"{k}: {v}" for k, v in summary.items()]

    # Monthly summary
    trades_df['month'] = pd.to_datetime(trades_df['entry_time']).dt.to_period('M')
    month_summary = trades_df.groupby('month').agg(
        total_trades=('pl', 'count'),
        win_rate=('result', lambda x: (x == 'WIN').mean()),
        pnl=('pl', 'sum')
    )

    output_lines.append("\n===== Monthly Summary =====")
    output_lines += month_summary.to_string().split("\n")

    folder = "/Users/aneeshviswanathan/Desktop/Om_Babaji_Om_threshold"
    i = 1
    while os.path.exists(os.path.join(folder, f"simulation_report_{i}.txt")):
        i += 1
    with open(os.path.join(folder, f"simulation_report_{i}.txt"), "w") as f:
        f.write("\n".join(output_lines))
    print(f"✅ Saved: simulation_report_{i}.txt")

def main():
    price = pd.read_csv(PRICE_CSV, index_col=0, parse_dates=True)
    raw = pd.read_csv(PRED_CSV, index_col=0, parse_dates=True)
    feat = pd.read_csv(FEAT_CSV, index_col=0, parse_dates=True)
    raw['long_conf'] = raw.apply(lambda x: x['confidence'] if x['direction'] == 'LONG' else 0.0, axis=1)
    raw['short_conf'] = raw.apply(lambda x: x['confidence'] if x['direction'] == 'SHORT' else 0.0, axis=1)
    pred = raw[['long_conf', 'short_conf']]
    sim = Simulator(price, pred, feat)
    sim.run()
    save_report(sim.results())

if __name__ == "__main__":
    main()