import pandas as pd
import os
from datetime import datetime, date, time, timedelta
import numpy as np

# === FINALIZED PARAMETERS ===
LONG_TH = 0.85
SHORT_TH = 0.85
ENTRY_START = "09:15"
ENTRY_END = "14:25"
FORCED_EXIT = "15:13"
START_DATE = "2024-01-01"
END_DATE = "2025-07-09"
FIXED_PROFIT_BOOKING = 50
FIXED_STOP_LOSS = 160
EXIT_SHORT_CONFIDENCE = 0.51
EXIT_LONG_CONFIDENCE = 0.51
ENABLE_ENTRY_SMOOTHING = True
ENTRY_SMOOTHING_WINDOW = 3
WEIGHTED_ENTRY_SMOOTHING = True
ENABLE_EXIT_SMOOTHING = True
EXIT_SMOOTHING_WINDOW = 15
WEIGHTED_EXIT_SMOOTHING = True
MAX_CONCURRENT_TRADES = 60
MAX_DAILY_TRADES = 70

# === PATHS ===
PRICE_CSV = "/Users/aneeshviswanathan/Desktop/Om_Babaji_Om_threshold/core_files/JAN2024_TO_JUN2025_BANKNIFTY_FUT.csv"
PRED_CSV = "/Users/aneeshviswanathan/Desktop/Om_Babaji_Om_threshold/core_files/model_predictions.csv"
FEAT_CSV = "/Users/aneeshviswanathan/Desktop/Om_Babaji_Om_threshold/core_files/EVAL_features_final.csv"
REPORT_FOLDER = "/Users/aneeshviswanathan/Desktop/Om_Babaji_Om_threshold"

def weighted_avg(series):
    weights = np.arange(1, len(series)+1)
    return np.dot(series, weights) / weights.sum()

def _parse_time(t_str):
    h, m = map(int, t_str.split(':'))
    return time(h, m)

def _parse_date(d_str):
    y, m, d = map(int, d_str.split('-'))
    return date(y, m, d)

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
        self.pl = (exit_price - self.entry_price) if self.direction == 'LONG' else (self.entry_price - exit_price)
        self.result = 'WIN' if self.pl > 0 else 'LOSS'

class Simulator:
    def __init__(self, price_df, pred_df, feat_df):
        self.price = price_df['close']
        self.pred = pred_df.copy()
        self.feat = feat_df
        self.trades = []
        self.open_trades = []
        self.entry_start = _parse_time(ENTRY_START)
        self.entry_end = _parse_time(ENTRY_END)
        self.forced_exit = _parse_time(FORCED_EXIT)
        self.start_date = _parse_date(START_DATE)
        self.end_date = _parse_date(END_DATE)

        if ENABLE_ENTRY_SMOOTHING:
            if WEIGHTED_ENTRY_SMOOTHING:
                self.pred['smoothed_long_conf'] = self.pred['long_conf'].rolling(ENTRY_SMOOTHING_WINDOW, min_periods=1).apply(weighted_avg, raw=True)
                self.pred['smoothed_short_conf'] = self.pred['short_conf'].rolling(ENTRY_SMOOTHING_WINDOW, min_periods=1).apply(weighted_avg, raw=True)
            else:
                self.pred['smoothed_long_conf'] = self.pred['long_conf'].rolling(ENTRY_SMOOTHING_WINDOW, min_periods=1).mean()
                self.pred['smoothed_short_conf'] = self.pred['short_conf'].rolling(ENTRY_SMOOTHING_WINDOW, min_periods=1).mean()
        else:
            self.pred['smoothed_long_conf'] = self.pred['long_conf']
            self.pred['smoothed_short_conf'] = self.pred['short_conf']

        if ENABLE_EXIT_SMOOTHING:
            if WEIGHTED_EXIT_SMOOTHING:
                self.pred['exit_smoothed_long_conf'] = self.pred['long_conf'].rolling(EXIT_SMOOTHING_WINDOW, min_periods=1).apply(weighted_avg, raw=True)
                self.pred['exit_smoothed_short_conf'] = self.pred['short_conf'].rolling(EXIT_SMOOTHING_WINDOW, min_periods=1).apply(weighted_avg, raw=True)
            else:
                self.pred['exit_smoothed_long_conf'] = self.pred['long_conf'].rolling(EXIT_SMOOTHING_WINDOW, min_periods=1).mean()
                self.pred['exit_smoothed_short_conf'] = self.pred['short_conf'].rolling(EXIT_SMOOTHING_WINDOW, min_periods=1).mean()
        else:
            self.pred['exit_smoothed_long_conf'] = self.pred['long_conf']
            self.pred['exit_smoothed_short_conf'] = self.pred['short_conf']

    def run(self):
        current = self.start_date
        while current <= self.end_date:
            prices = self.price.loc[datetime.combine(current, time.min):datetime.combine(current, time.max)]
            daily_trade_count = 0
            for ts, price_now in prices.items():
                ts_time = ts.time()

                # Forced exit
                if ts_time >= self.forced_exit:
                    for t in list(self.open_trades):
                        t.close(ts, price_now, reason='forced_exit')
                        self.trades.append(t)
                        self.open_trades.remove(t)

                # SL/TP check
                for t in list(self.open_trades):
                    f_tp = price_now >= t.entry_price + FIXED_PROFIT_BOOKING if t.direction == 'LONG' else price_now <= t.entry_price - FIXED_PROFIT_BOOKING
                    f_sl = price_now <= t.entry_price - FIXED_STOP_LOSS if t.direction == 'LONG' else price_now >= t.entry_price + FIXED_STOP_LOSS
                    if f_tp: t.close(ts, price_now, reason='fixed_tp')
                    elif f_sl: t.close(ts, price_now, reason='fixed_sl')
                    if t.exit_time: self.trades.append(t); self.open_trades.remove(t)

                # Confidence exit
                if ts in self.pred.index:
                    for t in list(self.open_trades):
                        long_conf = self.pred.at[ts, 'exit_smoothed_long_conf']
                        short_conf = self.pred.at[ts, 'exit_smoothed_short_conf']
                        if t.direction == 'LONG' and short_conf >= EXIT_SHORT_CONFIDENCE:
                            t.close(ts, price_now, reason='conf_short_exit')
                        elif t.direction == 'SHORT' and long_conf >= EXIT_LONG_CONFIDENCE:
                            t.close(ts, price_now, reason='conf_long_exit')
                        if t.exit_time: self.trades.append(t); self.open_trades.remove(t)

                # Entry condition
                if self.entry_start <= ts_time <= self.entry_end and len(self.open_trades) < MAX_CONCURRENT_TRADES and daily_trade_count < MAX_DAILY_TRADES:
                    if ts not in self.pred.index or ts not in self.feat.index:
                        continue
                    long_conf = self.pred.at[ts, 'smoothed_long_conf']
                    short_conf = self.pred.at[ts, 'smoothed_short_conf']
                    if long_conf >= LONG_TH:
                        sl = price_now - FIXED_STOP_LOSS
                        tp = price_now + FIXED_PROFIT_BOOKING
                        self.open_trades.append(Trade(ts, 'LONG', long_conf, price_now, sl, tp))
                        daily_trade_count += 1
                    elif short_conf >= SHORT_TH:
                        sl = price_now + FIXED_STOP_LOSS
                        tp = price_now - FIXED_PROFIT_BOOKING
                        self.open_trades.append(Trade(ts, 'SHORT', short_conf, price_now, sl, tp))
                        daily_trade_count += 1
            current += timedelta(days=1)

        # Final exit
        last_ts = self.price.index[-1]
        last_price = self.price.iloc[-1]
        for t in self.open_trades:
            t.close(last_ts, last_price, reason='final_close')
            self.trades.append(t)
        self.open_trades.clear()

    def results(self):
        return pd.DataFrame([{
            'entry_time': t.entry_time, 'exit_time': t.exit_time, 'direction': t.direction,
            'confidence': t.confidence, 'entry_price': t.entry_price, 'exit_price': t.exit_price,
            'pl': t.pl, 'result': t.result, 'exit_reason': t.exit_reason
        } for t in self.trades])

def save_report(trades_df):
    trades_df['date'] = pd.to_datetime(trades_df['entry_time']).dt.date
    trades_df['week'] = pd.to_datetime(trades_df['entry_time']).dt.isocalendar().week
    trades_df['month'] = pd.to_datetime(trades_df['entry_time']).dt.to_period('M')

    daily = trades_df.groupby('date').agg(trades=('pl','count'), win_rate=('result', lambda x: (x=='WIN').mean()), total_pl=('pl','sum'))
    weekly = trades_df.groupby('week').agg(trades=('pl','count'), win_rate=('result', lambda x: (x=='WIN').mean()), total_pl=('pl','sum'))
    monthly = trades_df.groupby('month').agg(trades=('pl','count'), win_rate=('result', lambda x: (x=='WIN').mean()), total_pl=('pl','sum'))

    summary = {
        'Period': f"{trades_df['entry_time'].min().date()} to {trades_df['exit_time'].max().date()}",
        'Total Trades': len(trades_df),
        'Win Rate': trades_df['result'].eq('WIN').mean(),
        'Total P/L': trades_df['pl'].sum()
    }

    output_lines = [
        "===== Trade Simulation Report =====",
        f"Period: {summary['Period']}",
        "\n===== Settings Used =====",
        f"LONG_TH = {LONG_TH}", f"SHORT_TH = {SHORT_TH}",
        f"MAX_CONCURRENT_TRADES = {MAX_CONCURRENT_TRADES}",
        f"MAX_DAILY_TRADES = {MAX_DAILY_TRADES}",
        f"ENTRY_START = {ENTRY_START}", f"ENTRY_END = {ENTRY_END}", f"FORCED_EXIT = {FORCED_EXIT}",
        f"FIXED_PROFIT_BOOKING = {FIXED_PROFIT_BOOKING}", f"FIXED_STOP_LOSS = {FIXED_STOP_LOSS}",
        f"EXIT_SHORT_CONFIDENCE = {EXIT_SHORT_CONFIDENCE}", f"EXIT_LONG_CONFIDENCE = {EXIT_LONG_CONFIDENCE}",
        f"ENTRY_SMOOTHING_WINDOW = {ENTRY_SMOOTHING_WINDOW} (Weighted={WEIGHTED_ENTRY_SMOOTHING})",
        f"EXIT_SMOOTHING_WINDOW = {EXIT_SMOOTHING_WINDOW} (Weighted={WEIGHTED_EXIT_SMOOTHING})",
        "\n===== Daily Report ====="
    ] + daily.to_string().split('\n') + ["\n===== Weekly Report ====="] + weekly.to_string().split('\n') + ["\n===== Monthly Report ====="] + monthly.to_string().split('\n') + ["\n===== Overall Summary ====="] + [f"{k}: {v}" for k,v in summary.items()]

    base_name = "simulation_report"
    i = 1
    while os.path.exists(os.path.join(REPORT_FOLDER, f"{base_name}_{i}.txt")):
        i += 1
    output_path = os.path.join(REPORT_FOLDER, f"{base_name}_{i}.txt")

    with open(output_path, 'w') as f:
        f.write('\n'.join(output_lines))
    print(f"✅ Simulation report saved to: {output_path}")

def main():
    price_df = pd.read_csv(PRICE_CSV, index_col=0, parse_dates=True)
    raw_pred = pd.read_csv(PRED_CSV, index_col=0, parse_dates=True)
    feat_df = pd.read_csv(FEAT_CSV, index_col=0, parse_dates=True)

    raw_pred['long_conf'] = raw_pred.apply(lambda x: x['confidence'] if x['direction'] == 'LONG' else 0.0, axis=1)
    raw_pred['short_conf'] = raw_pred.apply(lambda x: x['confidence'] if x['direction'] == 'SHORT' else 0.0, axis=1)
    pred_df = raw_pred[['long_conf', 'short_conf']]

    sim = Simulator(price_df, pred_df, feat_df)
    sim.run()
    trades_df = sim.results()
    save_report(trades_df)

if __name__ == '__main__':
    main()
