import logging
from broker_utils import _ensure_session, breeze
from datetime import datetime
import time

def fetch_and_display_vix_ltp():
    """
    Fetch live India VIX price every minute and display it in the format:
    INDIAVIX,YYYYMMDD,HH:MM,open,high,low,close,0,0
    Since LTP is used, all OHLC values will be the same.
    """
    _ensure_session()
    try:
        while True:
            now = datetime.now()
            date_str = now.strftime("%Y%m%d")
            time_str = now.strftime("%H:%M")

            response = breeze.get_quotes(
                stock_code="INDVIX",
                exchange_code="NSE",
                product_type="cash"
            )

            if isinstance(response, dict):
                success = response.get("Success")
                if isinstance(success, list) and success:
                    ltp = float(success[0].get("ltp", 0))
                    print(f"INDIAVIX,{date_str},{time_str},{ltp:.2f},{ltp:.2f},{ltp:.2f},{ltp:.2f},0,0")
                else:
                    print(f"❌ No valid quote data: {response}")
            else:
                print(f"❌ Unexpected API response: {response}")

            time.sleep(60)  # wait for 1 minute

    except KeyboardInterrupt:
        print("⏹️ Logging stopped by user.")
    except Exception:
        logging.exception("Failed to fetch VIX live price")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fetch_and_display_vix_ltp()