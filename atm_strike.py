# atm_strike.py
# Fetch BANKNIFTY spot price and calculate ATM strike,
# re-using the Breeze client from broker_utils.

import logging
from broker_utils import _ensure_session, breeze, SYMBOL_PREFIX
from trade_config import TradeConfig

log = logging.getLogger(__name__)

def get_banknifty_spot_index_price() -> float | None:
    """
    Lazily initialize the Breeze session, then fetch 
    the latest BANKNIFTY cash index LTP via BreezeConnect.
    Returns float or None on error.
    """
    _ensure_session()
    try:
        resp = breeze.get_quotes(
            stock_code=SYMBOL_PREFIX,
            exchange_code="NSE",
            product_type="cash"
        )
        if not isinstance(resp, dict):
            log.error("Unexpected spot response (not a dict): %r", resp)
            return None

        success = resp.get("Success") or []
        if isinstance(success, list) and success:
            ltp = success[0].get("ltp")
            if ltp is not None:
                return float(ltp)

    except Exception:
        log.exception("Error fetching BANKNIFTY spot price")
    return None

def calculate_atm_strike(
    spot_price: float,
    interval: int = TradeConfig.ATM_STRIKE_ROUNDING
) -> int:
    """
    Round spot_price to the nearest 'interval' for ATM strike.
    Uses TradeConfig.ATM_STRIKE_ROUNDING by default.
    """
    return int(round(spot_price / interval)) * interval

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    price = get_banknifty_spot_index_price()
    if price is not None:
        atm = calculate_atm_strike(price)
        print(f"✅ Spot: ₹{price:.2f}, ATM Strike: ₹{atm}")
    else:
        print("⚠️ Failed to fetch BANKNIFTY spot price")
