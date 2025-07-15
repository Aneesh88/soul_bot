"""
exit_strategies.py

Modular exit strategies for trade_manager, each returning a StrategyResult tuple.
"""
from collections import namedtuple
from datetime import time as dt_time
import pandas as pd


from trade_config import TradeConfig
from broker_utils import get_option_ltp

# Define the result type for exit strategies
StrategyResult = namedtuple('StrategyResult', ['should_exit', 'reason', 'price'])

def fixed_sl_tp(trade: dict, futures_price: float, **kwargs) -> StrategyResult:
    """Exit when futures price hits hardcoded fixed SL/TP from TradeConfig."""
    direction = trade.get('direction')
    entry = trade.get('entry_index_price', 0.0)  # ✅ FIXED: use correct entry price
    if direction == 'LONG':
        sl = entry - TradeConfig.FIXED_SL
        tp = entry + TradeConfig.FIXED_TP
        if futures_price <= sl:
            return StrategyResult(True, 'FIXED_SL', futures_price)
        if futures_price >= tp:
            return StrategyResult(True, 'FIXED_TP', futures_price)
    else:  # SHORT
        sl = entry + TradeConfig.FIXED_SL
        tp = entry - TradeConfig.FIXED_TP
        if futures_price >= sl:
            return StrategyResult(True, 'FIXED_SL', futures_price)
        if futures_price <= tp:
            return StrategyResult(True, 'FIXED_TP', futures_price)
    return StrategyResult(False, None, 0.0)


def exit_on_prediction_confidence(trade: dict, prediction_row: pd.Series, **kwargs) -> StrategyResult:
    """Opposite-side confidence exit (simulator style).
    Exits a LONG when short_conf ≥ EXIT_SHORT_CONF,
    and exits a SHORT when long_conf ≥ EXIT_LONG_CONF.
    """
    if prediction_row is None or prediction_row.empty:
        return StrategyResult(False, None, 0.0)

    long_conf  = prediction_row.get('exit_smoothed_long_conf', 0.0)
    short_conf = prediction_row.get('exit_smoothed_short_conf', 0.0)
    direction  = trade.get('direction')

    if direction == 'LONG' and short_conf >= TradeConfig.EXIT_SHORT_CONF:
        return StrategyResult(True, 'CONF_EXIT_SHORT', 0.0)
    if direction == 'SHORT' and long_conf >= TradeConfig.EXIT_LONG_CONF:
        return StrategyResult(True, 'CONF_EXIT_LONG', 0.0)

    return StrategyResult(False, None, 0.0)



def eod_force(trade: dict, now_time: dt_time, futures_price: float = 0.0, **kwargs) -> StrategyResult:
    """Force exit any open trades at or after the configured end-of-day time."""
    eod_time = dt_time.fromisoformat(TradeConfig.FORCED_EXIT)
    if now_time >= eod_time:
        return StrategyResult(True, 'EOD', futures_price)
    return StrategyResult(False, None, 0.0)

# Ordered list of exit strategies to apply
ALL_STRATEGIES = [
    fixed_sl_tp,
    exit_on_prediction_confidence,
    eod_force,
]
