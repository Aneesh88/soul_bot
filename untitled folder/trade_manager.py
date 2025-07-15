#trade_manager.py
import os
import time
import logging
import csv
import json
from datetime import datetime, date, time as dt_time

import pandas as pd

from trade_config import TradeConfig
from db import get_conn

# Order placement and LTP helper
from broker_utils import entry_order, exit_order, get_option_ltp, SYMBOL_PREFIX

# Index prices and ATM strike calculation
from true_data_utils import get_banknifty_futures_price
from atm_strike import get_banknifty_spot_index_price, calculate_atm_strike

# Exit strategies framework
from exit_strategies import ALL_STRATEGIES, StrategyResult

# Only import the trade-state loader from state_manager
from state_manager import load_trade_state

# Telegram notifications
from telegram import send_entry_notification, send_exit_notification, send_message as safe_send

log     = logging.getLogger("trade_manager")
console = logging.getLogger("console")





# History CSV configuration
HISTORY_CSV = TradeConfig.TRADE_HISTORY_CSV
HISTORY_FIELDS = [
    "trade_number",
    "direction",
    "confidence",
    "instrument",
    "quantity",
    "entry_price_option",
    "exit_price_option",
    "result",          # “WIN” or “LOSS”
    "pnl",
    "entry_time",
    "exit_time",
    "exit_reason"
]


import pandas as pd
from datetime import datetime
from db import get_conn

def load_predictions() -> pd.DataFrame:
    """
    Fetch all today's predictions from new_predictions table,
    filtering by local date in Python rather than in SQL.
    """
    sql = """
    SELECT *
      FROM new_predictions
     ORDER BY timestamp
    """
    with get_conn() as conn:
        df = pd.read_sql(sql, conn, parse_dates=["timestamp"])

    # If no data at all, just return empty DF
    if df.empty:
        return df

    # Filter to only keep rows whose timestamp falls on today's date
    today = datetime.now().date()
    df = df[df["timestamp"].dt.date == today].reset_index(drop=True)
    return df




def load_features() -> pd.DataFrame:
    """
    Load all today’s features into a DataFrame indexed by timestamp.
    Only used if additional context is needed.
    """
    with get_conn() as conn:
        df = pd.read_sql(
            "SELECT timestamp FROM features ORDER BY timestamp",
            conn,
            parse_dates=["timestamp"]
        )
    df.set_index("timestamp", inplace=True)
    return df


def append_history(record: dict):
    """
    Append a single EXIT-only row to trade_history.csv,
    using only the fields in HISTORY_FIELDS and in that exact order.
    """
    os.makedirs(os.path.dirname(HISTORY_CSV), exist_ok=True)
    write_header = not os.path.exists(HISTORY_CSV)

    with open(HISTORY_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HISTORY_FIELDS)
        if write_header:
            writer.writeheader()
        row = {k: record.get(k, "") for k in HISTORY_FIELDS}
        writer.writerow(row)







def entry_manager():
    """Handle new entries, now persisting trades & state in SQLite tables,
       and recording both raw and smoothed confidence values."""
    try:
        # 1) Load today's model signals
        df_pred = load_predictions()
        if df_pred.empty:
            console.debug("No predictions for today, skipping entries.")
            return

        # 2) (Optional) Load feature regime
        df_feat = load_features()

        # 3) Ensure today's state row exists (or reset if date changed)
        state = load_trade_state()
        today = date.today().isoformat()
        if state.get("date") != today:
            state = {
                "date":               today,
                "last_trade_number":  0,
                "active_trade_count": 0,
                "closed_trade_count": 0,
                "win_count":          0,
                "loss_count":         0,
                "daily_pnl":          0.0
            }
            with get_conn() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO daily_trade_state
                      (date, last_trade_number, active_trade_count,
                       closed_trade_count, win_count, loss_count, daily_pnl)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    state["date"],
                    state["last_trade_number"],
                    state["active_trade_count"],
                    state["closed_trade_count"],
                    state["win_count"],
                    state["loss_count"],
                    state["daily_pnl"]
                ))
                conn.commit()
            console.debug("Initialized daily state for %s", today)

        # 4) Fetch currently-open timestamps
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT timestamp FROM live_trade_details WHERE status = 'OPEN'")
            existing_ts = {pd.to_datetime(r[0]) for r in cur.fetchall()}
        console.debug("Currently open trades at timestamps: %s", existing_ts)

        # 5) Time window & trade caps
        now_time = datetime.now().time()
        start    = dt_time.fromisoformat(TradeConfig.ENTRY_START)
        end      = dt_time.fromisoformat(TradeConfig.ENTRY_END)
        console.debug("ENTRY window? now=%s start=%s end=%s", now_time, start, end)
        if not (start <= now_time <= end):
            return
        if state["active_trade_count"] >= TradeConfig.MAX_CONCURRENT_TRADES:
            console.debug("Max concurrent trades reached (%d), skipping entries.",
                          state["active_trade_count"])
            return
        if state["last_trade_number"] >= TradeConfig.MAX_TRADES_PER_DAY:
            console.debug("Daily trade cap reached (%d), skipping entries.",
                          state["last_trade_number"])
            return

        # 6) Market data
        spot    = get_banknifty_spot_index_price()
        fut_ltp = get_banknifty_futures_price()
        if spot is None or fut_ltp is None:
            safe_send("⚠️ Entry skipped – cannot fetch market price")
            return
        atm_strike = calculate_atm_strike(spot, TradeConfig.ATM_STRIKE_ROUNDING)
        console.debug("Spot=%s, Futures LTP=%s, ATM Strike=%s", spot, fut_ltp, atm_strike)

        # 7) Loop through prediction rows
        for _, row in df_pred.iterrows():
            tsig    = row["timestamp"]
            tsig_dt = pd.to_datetime(tsig)

            # skip already-open
            if tsig_dt in existing_ts:
                console.debug("Skipping %s – already open", tsig_dt)
                continue

            # skip stale
            age      = (datetime.now() - tsig_dt).total_seconds()
            raw_conf = row.get("confidence", 0.0)
            console.debug("Signal @%s age=%.0fs raw_conf=%.3f",
                          tsig_dt.time(), age, raw_conf)
            if age > TradeConfig.ENTRY_MAX_SIGNAL_AGE:
                console.debug("Skipping %s – age %.0fs > %ds",
                              tsig_dt, age, TradeConfig.ENTRY_MAX_SIGNAL_AGE)
                continue

            # determine direction, smoothed confidence & threshold
            dirn = row["direction"]
            if dirn == "LONG":
                sm_conf = row.get("entry_smoothed_long_conf", 0.0)
                thr     = TradeConfig.LONG_TH
                if sm_conf < thr:
                    console.debug("Skipping %s LONG – smoothed=%.3f < %.2f",
                                  tsig_dt, sm_conf, thr)
                    continue
            else:
                sm_conf = row.get("entry_smoothed_short_conf", 0.0)
                thr     = TradeConfig.SHORT_TH
                if sm_conf < thr:
                    console.debug("Skipping %s SHORT – smoothed=%.3f < %.2f",
                                  tsig_dt, sm_conf, thr)
                    continue

            # compute SL/TP
            if dirn == "LONG":
                sl = fut_ltp - TradeConfig.FIXED_SL
                tp = fut_ltp + TradeConfig.FIXED_TP
            else:
                sl = fut_ltp + TradeConfig.FIXED_SL
                tp = fut_ltp - TradeConfig.FIXED_TP

            # place entry order
            breeze_opt = "call" if dirn == "LONG" else "put"
            resp = entry_order(
                quantity    = TradeConfig.ORDER_QUANTITY,
                option_type = breeze_opt,
                strike      = atm_strike
            )
            oid       = resp.get("order_id")
            price_opt = resp.get("entry_price", 0.0)
            if not oid:
                safe_send(f"❌ ENTRY FAILED @{tsig}")
                continue

            # 8) Persist trade with both confidences
            tn         = state["last_trade_number"] + 1
            ts_str     = tsig_dt.strftime("%Y-%m-%d %H:%M:%S")
            entry_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            instrument = f"{SYMBOL_PREFIX}{atm_strike}{'CE' if dirn=='LONG' else 'PE'}"
            with get_conn() as conn:
                conn.execute("""
                    INSERT INTO live_trade_details
                      (trade_number, timestamp, entry_time, entry_order_id,
                       direction, confidence, raw_confidence,
                       instrument, quantity,
                       entry_index_price, entry_price_option,
                       fut_index_sl_level, fut_index_tp_level,
                       strike, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
                """, (
                    tn,
                    ts_str,
                    entry_time,
                    oid,
                    dirn,
                    sm_conf,
                    raw_conf,
                    instrument,
                    TradeConfig.ORDER_QUANTITY,
                    round(fut_ltp, 2),
                    price_opt,
                    round(sl, 2),
                    round(tp, 2),
                    atm_strike
                ))
                # update state
                state["last_trade_number"] += 1
                state["active_trade_count"] += 1
                conn.execute("""
                    UPDATE daily_trade_state
                      SET last_trade_number = ?, active_trade_count = ?
                      WHERE date = ?
                """, (
                    state["last_trade_number"],
                    state["active_trade_count"],
                    today
                ))
                conn.commit()

            # 9) Notify & log – now including the threshold
            send_entry_notification({
                "trade_number":        tn,
                "entry_time":          entry_time,
                "direction":           dirn,
                "raw_confidence":      raw_conf,
                "smoothed_confidence": sm_conf,
                "threshold":           thr,
                "strike":              atm_strike,
                "quantity":            TradeConfig.ORDER_QUANTITY,
                "entry_price_option":  price_opt,
                "fut_index_sl_level":  round(sl, 2),
                "fut_index_tp_level":  round(tp, 2)
            })
            console.info(
                f"⏱️ [{ts_str[:16]}] ➕ ENTRY #{tn} {dirn} @ {price_opt:.2f} "
                f"(raw={raw_conf:.2f}, sm={sm_conf:.2f}, th={thr:.2f})"
            )

    except Exception:
        log.exception("entry_manager error")










def exit_manager():
    """Handle exits using all configured strategies, archive closed trades, and log exits."""
    try:
        # 1) Load open trades
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM live_trade_details WHERE status='OPEN'")
            rows = cur.fetchall()
            cols = [col[0] for col in cur.description]
            live_trades = [dict(zip(cols, r)) for r in rows]
        if not live_trades:
            return

        # 2) Gather market data & state
        futures_price = get_banknifty_futures_price()
        if futures_price is None:
            safe_send("⚠️ Exit skipped – cannot fetch futures index price")
            return

        now_time = datetime.now().time()
        state = load_trade_state()

        # 3) Load today's prediction data
        df_pred = load_predictions()
        pred_map = {
            pd.to_datetime(row['timestamp']): row
            for _, row in df_pred.iterrows()
        }

        # 4) Evaluate each open trade
        for tr in live_trades:
            exit_price = None
            reason     = None

            # Match prediction row by timestamp
            prediction_row = pred_map.get(pd.to_datetime(tr['timestamp']))

            # Apply exit strategies in order
            for strat in ALL_STRATEGIES:
                res = strat(
                    trade          = tr,
                    futures_price  = futures_price,
                    prediction_row = prediction_row,
                    now_time       = now_time
                )
                if res.should_exit:
                    reason     = res.reason
                    exit_price = res.price
                    break

            if not reason:
                continue

            # 5) Place the exit order
            breeze_opt = 'call' if tr['instrument'].endswith('CE') else 'put'
            if not tr.get('strike'):
                safe_send(f"❌ EXIT FAILED for Trade# {tr['trade_number']} – Missing strike info")
                continue

            console.info(f"🚨 Attempting EXIT for {tr['instrument']} | Strike={tr['strike']}, Type={breeze_opt}")
            resp = exit_order(
                quantity    = tr['quantity'],
                option_type = breeze_opt,
                strike      = tr['strike']
            )
            exit_id           = resp.get('order_id')
            exit_price_option = resp.get('exit_price', exit_price or 0.0)
            if not exit_id:
                safe_send(f"❌ EXIT FAILED for Trade# {tr['trade_number']}")
                continue

            # 6) Compute P&L and result
            entry_opt  = tr.get('entry_price_option', 0.0)
            option_pnl = round((exit_price_option - entry_opt) * tr['quantity'], 2)
            index_pnl  = round((futures_price - tr['entry_index_price']) * tr['quantity'], 2)
            result     = 'WIN' if option_pnl >= 0 else 'LOSS'

            # 7) Persist updates
            exit_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("""
                  UPDATE live_trade_details
                     SET exit_time          = ?,
                         exit_order_id      = ?,
                         exit_price_option  = ?,
                         exit_index_price   = ?,
                         option_pnl         = ?,
                         index_pnl          = ?,
                         exit_reason        = ?,
                         status             = 'CLOSED'
                   WHERE trade_number = ?
                """, (
                  exit_time,
                  exit_id,
                  exit_price_option,
                  futures_price,
                  option_pnl,
                  index_pnl,
                  reason,
                  tr['trade_number']
                ))

                # update daily state
                state['active_trade_count'] -= 1
                state['closed_trade_count'] += 1
                state['daily_pnl']          += option_pnl
                if option_pnl >= 0:
                    state['win_count'] += 1
                else:
                    state['loss_count'] += 1

                cur.execute("""
                  UPDATE daily_trade_state
                     SET active_trade_count = ?,
                         closed_trade_count = ?,
                         win_count          = ?,
                         loss_count         = ?,
                         daily_pnl          = ?
                   WHERE date = ?
                """, (
                  state['active_trade_count'],
                  state['closed_trade_count'],
                  state['win_count'],
                  state['loss_count'],
                  state['daily_pnl'],
                  state['date']
                ))
                conn.commit()

            # 8) Archive and notify
            record = {
                **tr,
                'exit_time'         : exit_time,
                'exit_order_id'     : exit_id,
                'exit_price_option' : exit_price_option,
                'exit_index_price'  : futures_price,
                'option_pnl'        : option_pnl,
                'index_pnl'         : index_pnl,
                'exit_reason'       : reason,
                'status'            : 'CLOSED',
                'result'            : result
            }
            archive_path = TradeConfig.CLOSED_TRADES_JSON
            archive_path.parent.mkdir(exist_ok=True)
            if not archive_path.exists():
                archive_path.write_text('[]')
            archive = json.loads(archive_path.read_text())
            archive.append(record)
            archive_path.write_text(json.dumps(archive, indent=2))

            append_history(record)

            # ensure exit notification fires even if file I/O fails
            console.info(f"🔔 About to send EXIT notification for Trade#{tr['trade_number']}")
            try:
                send_exit_notification(record, state['daily_pnl'])
            except Exception as e:
                log.error(f"Failed to send exit notification: {e}")

            console.info(f"⏱️ [{exit_time[:16]}] ➖ EXIT #{tr['trade_number']} P&L={option_pnl:.2f}")

    except Exception:
        log.exception("exit_manager error")








def archive_closed_trade(trade: dict):
    """
    Archive a closed trade by updating its row in live_trade_details.
    """
    try:
        from db import get_conn

        with get_conn() as conn:
            conn.execute("""
                UPDATE live_trade_details
                   SET exit_time          = ?,
                       exit_order_id      = ?,
                       exit_price_option  = ?,
                       pnl                = ?,
                       exit_reason        = ?,
                       status             = 'CLOSED'
                 WHERE trade_number = ?
            """, (
                trade['exit_time'],
                trade['exit_order_id'],
                trade['exit_price_option'],
                trade['pnl'],
                trade['exit_reason'],
                trade['trade_number']
            ))
            conn.commit()
    except Exception:
        logging.getLogger(__name__).exception("archive_closed_trade error")




def run_manager():
    logging.basicConfig(level=logging.INFO)
    log.info("Starting trade_manager")
    try:
        while True:
            entry_manager()
            exit_manager()
            time.sleep(10)
    except KeyboardInterrupt:
        log.info("Stopping trade_manager")



if __name__ == "__main__":
    run_manager()
