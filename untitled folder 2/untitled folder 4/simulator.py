"""
simulator.py

Standalone backtesting simulator for index-level trading:
- Confidence thresholds
- ATR-based SL/TP
- EMA filter
- Daily time windows and forced exit
- Additional EMA-based reversal exit layer
All parameters and file paths are defined at the top—no external inputs needed.
"""

import pandas as pd
import sys
from datetime import datetime, date, time, timedelta

# ─────────────────────────────────────────────────────────────────────────────
# PARAMETERS (Edit these to change simulation behavior)
# ─────────────────────────────────────────────────────────────────────────────
LONG_TH = 0.85              # min probability to open LONG
SHORT_TH = 0.75            # min probability to open SHORT
ATR_MUL = 5               # ATR multiplier for stop-loss
RR_RATIO = 2           # min Risk/Reward ratio (TP/SL)
MAX_CONCURRENT_TRADES = 6   # max open trades at once
ENTRY_START = "09:20"      # HH:MM, first allowed entry
ENTRY_END   = "14:40"      # HH:MM, last allowed entry
FORCED_EXIT = "15:13"      # HH:MM, end-of-day forced exit
START_DATE  = "2025-07-08" # YYYY-MM-DD, simulation start date
END_DATE    = "2025-07-08" # YYYY-MM-DD, simulation end date

# ─────────────────────────────────────────────────────────────────────────────
# EMA-based reversal exit settings
# ─────────────────────────────────────────────────────────────────────────────
EXIT_ON_EMA_REVERSAL = False  # enable exit when EMA filter reverses
EMA_REVERSAL_COUNT  = 3       # number of consecutive bars to trigger exit

# ─────────────────────────────────────────────────────────────────────────────
# File paths (hardcoded)
# ─────────────────────────────────────────────────────────────────────────────
PRICE_CSV = "/Users/aneeshviswanathan/Desktop/Updated_om_babaji_om/core_files/JAN2024_TO_JUN2025_BANKNIFTY_FUT.csv"
PRED_CSV  = "/Users/aneeshviswanathan/Desktop/Updated_om_babaji_om/core_files/model_predictions.csv"
FEAT_CSV  = "/Users/aneeshviswanathan/Desktop/Updated_om_babaji_om/core_files/EVAL_features_final.csv"
# ─────────────────────────────────────────────────────────────────────────────


def _parse_time(t_str: str) -> time:
    h, m = map(int, t_str.split(':'))
    return time(h, m)


def _parse_date(d_str: str) -> date:
    y, mo, d = map(int, d_str.split('-'))
    return date(y, mo, d)


class Trade:
    """Represents a single trade's lifecycle."""
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

    def close(self, exit_time, exit_price, reason=None):
        self.exit_time = exit_time
        self.exit_price = exit_price
        if self.direction == 'LONG':
            self.pl = exit_price - self.entry_price
        else:
            self.pl = self.entry_price - exit_price
        self.result = 'WIN' if self.pl > 0 else 'LOSS'
        # we could record reason if desired


class Simulator:
    """Runs the backtest simulation given price, predictions, and features."""
    def __init__(self, price_df, pred_df, feat_df):
        self.price = price_df['close']
        self.pred = pred_df
        self.feat = feat_df
        self.trades = []
        self.open_trades = []

        # parse config
        self.entry_start = _parse_time(ENTRY_START)
        self.entry_end   = _parse_time(ENTRY_END)
        self.forced_exit = _parse_time(FORCED_EXIT)
        self.start_date  = _parse_date(START_DATE)
        self.end_date    = _parse_date(END_DATE)

    def run(self):
        current = self.start_date
        while current <= self.end_date:
            day_start = datetime.combine(current, time(0,0))
            day_end   = datetime.combine(current, time(23,59))
            prices = self.price.loc[day_start:day_end]

            for ts, price_now in prices.items():
                ts_time = ts.time()

                # 1) Forced exit at end-of-day
                if ts_time >= self.forced_exit and self.open_trades:
                    for t in list(self.open_trades):
                        t.close(ts, price_now)
                        self.trades.append(t)
                        self.open_trades.remove(t)

                # 2) SL/TP exit checks
                for t in list(self.open_trades):
                    if t.direction == 'LONG' and (price_now <= t.sl or price_now >= t.tp):
                        t.close(ts, price_now)
                        self.trades.append(t)
                        self.open_trades.remove(t)
                    elif t.direction == 'SHORT' and (price_now >= t.sl or price_now <= t.tp):
                        t.close(ts, price_now)
                        self.trades.append(t)
                        self.open_trades.remove(t)

                # 3) EMA-based reversal exit
                if EXIT_ON_EMA_REVERSAL and self.open_trades:
                    for t in list(self.open_trades):
                        # gather last N ema_filter_15 flags up to current ts
                        flags = self.feat.loc[:ts, 'ema_filter_15']
                        last_flags = flags.tail(EMA_REVERSAL_COUNT)
                        if len(last_flags) == EMA_REVERSAL_COUNT:
                            # LONG exit when consecutive 0's
                            if t.direction == 'LONG' and (last_flags == 0).all():
                                t.close(ts, price_now)
                                self.trades.append(t)
                                self.open_trades.remove(t)
                            # SHORT exit when consecutive 1's
                            elif t.direction == 'SHORT' and (last_flags == 1).all():
                                t.close(ts, price_now)
                                self.trades.append(t)
                                self.open_trades.remove(t)

                # 4) Entry logic within window
                if (self.entry_start <= ts_time <= self.entry_end
                    and len(self.open_trades) < MAX_CONCURRENT_TRADES):

                    long_conf  = self.pred.at[ts, 'long_conf']
                    short_conf = self.pred.at[ts, 'short_conf']
                    trend      = self.feat.at[ts, 'ema_filter_15']
                    atr        = self.feat.at[ts, 'atr']

                    # LONG entry
                    if trend == 1 and long_conf >= LONG_TH:
                        sl = price_now - ATR_MUL * atr
                        tp = price_now + ATR_MUL * atr * RR_RATIO
                        self.open_trades.append(
                            Trade(ts, 'LONG', long_conf, price_now, sl, tp)
                        )
                    # SHORT entry
                    elif trend == 0 and short_conf >= SHORT_TH:
                        sl = price_now + ATR_MUL * atr
                        tp = price_now - ATR_MUL * atr * RR_RATIO
                        self.open_trades.append(
                            Trade(ts, 'SHORT', short_conf, price_now, sl, tp)
                        )

            current += timedelta(days=1)

        # Final close for any remaining trades
        if self.open_trades:
            last_ts = self.price.index[-1]
            last_price = self.price.iloc[-1]
            for t in self.open_trades:
                t.close(last_ts, last_price)
                self.trades.append(t)
            self.open_trades.clear()

    def results(self):
        df = pd.DataFrame([{
            'entry_time': t.entry_time,
            'exit_time' : t.exit_time,
            'direction' : t.direction,
            'confidence': t.confidence,
            'entry_price': t.entry_price,
            'exit_price' : t.exit_price,
            'pl'        : t.pl,
            'result'    : t.result
        } for t in self.trades])
        summary = {
            'total_trades': len(df),
            'win_rate'    : df['result'].eq('WIN').mean(),
            'total_pl'    : df['pl'].sum()
        }
        return df, summary


def main():
    # Load CSV data
    try:
        price_df = pd.read_csv(PRICE_CSV, index_col=0, parse_dates=True)
        raw_pred = pd.read_csv(PRED_CSV, index_col=0, parse_dates=True)
        feat_df  = pd.read_csv(FEAT_CSV,  index_col=0, parse_dates=True)
    except Exception as e:
        print(f"Error loading CSVs: {e}", file=sys.stderr)
        sys.exit(1)

    # Transform prediction format: direction + confidence → long_conf, short_conf
    raw_pred['long_conf']  = raw_pred.apply(
        lambda x: x['confidence'] if x['direction']=='LONG' else 0.0, axis=1)
    raw_pred['short_conf'] = raw_pred.apply(
        lambda x: x['confidence'] if x['direction']=='SHORT' else 0.0, axis=1)
    pred_df = raw_pred[['long_conf','short_conf']]

    # Validate feature columns
    missingf = [c for c in ['atr','ema_filter_15'] if c not in feat_df.columns]
    if missingf:
        print(f"Feature CSV missing columns: {missingf}\nAvailable: {list(feat_df.columns)}", file=sys.stderr)
        sys.exit(1)

    sim = Simulator(price_df, pred_df, feat_df)
    sim.run()
    trades_df, summary = sim.results()

    print("===== Trades =====")
    print(trades_df.to_string(index=False))
    print("===== Summary =====")
    for k,v in summary.items(): print(f"{k}: {v}")


if __name__=='__main__':
    main()
