# broker_utils.py
# Entry/exit order functions and option LTP helper for ICICI Breeze platform,
# using centralized config and standardized logging.

import logging
import time
import json

from breeze_connect import BreezeConnect
from trade_config import TradeConfig

log = logging.getLogger(__name__)

# Initialize Breeze client facade (session will be generated lazily)
breeze = BreezeConnect(api_key=TradeConfig.BREEZE_KEY)
_session_initialized = False

# Hardcoded option symbol prefix for BANKNIFTY
SYMBOL_PREFIX = "CNXBAN"


def _ensure_session():
    """
    Lazily initialize Breeze session. Subsequent calls will be no-ops.
    """
    global _session_initialized
    if _session_initialized:
        return
    try:
        breeze.generate_session(
            api_secret=TradeConfig.BREEZE_SECRET,
            session_token=TradeConfig.BREEZE_TOKEN
        )
        _session_initialized = True
        log.info("Breeze session initialized successfully")
    except Exception as e:
        log.error("Could not initialize Breeze session: %s", e)
        # Do not raise; subsequent order calls will retry session init


def entry_order(
    quantity: int,
    option_type: str,
    strike: float
) -> dict:
    """
    Place a market BUY order for the BANKNIFTY option.
    Returns {'order_id': str or None, 'entry_price': float}.
    """
    _ensure_session()
    try:
        quantity_int = int(quantity)
        strike_int = int(strike)

        payload_summary = (
            f"🚨 Final Order Payload → qty={quantity_int}, "
            f"strike={strike_int}, right={option_type}, "
            f"expiry={TradeConfig.BREEZE_EXPIRY}, symbol={SYMBOL_PREFIX}"
        )
        print(payload_summary)
        log.info(payload_summary)

        resp = breeze.place_order(
            exchange_code="NFO",
            stock_code=SYMBOL_PREFIX,
            product="options",
            action="buy",
            order_type="market",
            validity="day",
            quantity=quantity_int,
            expiry_date=TradeConfig.BREEZE_EXPIRY,
            right=option_type,
            strike_price=strike_int
        )

        if not isinstance(resp, dict):
            log.error("Unexpected broker response (not a dict): %r", resp)
            return {"order_id": None, "entry_price": 0.0}

        success = resp.get("Success") or {}
        order_id = success.get("order_id")
        if not order_id:
            log.error("BUY order failed, response=%r", resp)
            return {"order_id": None, "entry_price": 0.0}

        entry_price = 0.0
        for _ in range(3):
            detail = breeze.get_order_detail(order_id=order_id, exchange_code="NFO")
            if isinstance(detail, str):
                detail = json.loads(detail)
            block = detail.get("Success") or []
            if block:
                avg = float(block[0].get("average_price", 0) or 0)
                if avg > 0:
                    entry_price = avg
                    break
            time.sleep(1)

        log.info("[BUY] order_id=%s, entry_price=%.2f", order_id, entry_price)
        return {"order_id": order_id, "entry_price": entry_price}

    except Exception:
        log.exception("Exception placing BUY order")
        return {"order_id": None, "entry_price": 0.0}



def exit_order(
    quantity: int,
    option_type: str,
    strike: float
) -> dict:
    """
    Place a market SELL order to exit the BANKNIFTY option position.
    Returns {'order_id': str or None, 'exit_price': float}.
    """
    _ensure_session()
    try:
        resp = breeze.place_order(
            exchange_code="NFO",
            stock_code=SYMBOL_PREFIX,
            product="options",
            action="sell",
            order_type="market",
            validity="day",
            quantity=quantity,
            expiry_date=TradeConfig.BREEZE_EXPIRY,
            right=option_type,
            strike_price=strike
        )
        if not isinstance(resp, dict):
            log.error("Unexpected broker response (not a dict) on SELL: %r", resp)
            return {"order_id": None, "exit_price": 0.0}

        success = resp.get("Success") or {}
        exit_id = success.get("order_id")
        if not exit_id:
            log.error("SELL order failed, response=%r", resp)
            return {"order_id": None, "exit_price": 0.0}

        exit_price = 0.0
        for _ in range(3):
            detail = breeze.get_order_detail(order_id=exit_id, exchange_code="NFO")
            if isinstance(detail, str):
                detail = json.loads(detail)
            block = detail.get("Success") or []
            if block:
                avg = float(block[0].get("average_price", 0) or 0)
                if avg > 0:
                    exit_price = avg
                    break
            time.sleep(1)

        log.info("[SELL] order_id=%s, exit_price=%.2f", exit_id, exit_price)
        return {"order_id": exit_id, "exit_price": exit_price}

    except Exception:
        log.exception("Exception placing SELL order")
        return {"order_id": None, "exit_price": 0.0}


# New function: get live option LTP for percent-based exits
def get_option_ltp(
    strike: int,
    right: str = "CE"
) -> float:
    """
    Returns the current last-traded price (LTP) for the BANKNIFTY option at `strike` and `right`.
    """
    _ensure_session()

    payload = {
        "exchange_code": "NFO",
        "stock_code": SYMBOL_PREFIX,
        "product_type": "Options",
        "expiry_date": TradeConfig.BREEZE_EXPIRY,
        "right": "Call" if right.upper() == "CE" else "Put",
        "strike_price": strike
    }
    log.info(f"Fetching option LTP with payload: {payload}")

    resp = breeze.get_quotes(**payload)
    items = resp.get("Success") or []
    if not items:
        raise ValueError(f"No market data returned for {strike}{right}")

    data = items[0]
    ltp = data.get("ltp") or data.get("last_traded_price") or data.get("last_price")
    if ltp is None:
        raise ValueError(f"No LTP field found in response for {strike}{right}")

    return float(ltp)
