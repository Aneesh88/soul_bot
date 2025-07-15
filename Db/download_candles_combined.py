import requests
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import io

# ────── USER CONFIG ───────────────────────
USERNAME     = "tdwsp674"
PASSWORD     = "aneesh@674"

START_DATE   = datetime(2023, 10, 1)
END_DATE     = datetime.now()

MARKET_OPEN  = "T09:15:00"
MARKET_CLOSE = "T15:30:00"

OUTPUT_FILE  = f"BANKNIFTY_FUT_{START_DATE.strftime('%b%Y')}_TO_{END_DATE.strftime('%b%d%Y')}.csv"

# Expiry dates including 2023 and 2024
expiry_dates = [
    (datetime.strptime(d, "%d/%m/%Y"), f"BANKNIFTY{datetime.strptime(d, '%d/%m/%Y').strftime('%y%b').upper()}FUT")
    for d in [
        "26/10/2023", "30/11/2023", "28/12/2023", "25/01/2024",
        "29/02/2024", "27/03/2024", "24/04/2024", "29/05/2024", "26/06/2024",
        "31/07/2024", "28/08/2024", "25/09/2024", "30/10/2024", "27/11/2024", "24/12/2024",
        "30/01/2025", "27/02/2025", "27/03/2025", "24/04/2025", "29/05/2025", "26/06/2025","31/07/2025"
    ]
]

script_dir = Path(__file__).parent
all_data = []

def authenticate():
    auth_url = "https://auth.truedata.in/token"
    payload = {"username": USERNAME, "password": PASSWORD, "grant_type": "password"}
    resp = requests.post(auth_url, data=payload)
    resp.raise_for_status()
    return resp.json().get("access_token")

def get_symbol_by_date(date: datetime) -> str:
    for expiry, symbol in expiry_dates:
        if date.date() <= expiry.date():
            return symbol
    raise ValueError(f"No future defined for date {date.date()}")

def fetch_day_data(token: str, symbol: str, date: datetime):
    day_str  = date.strftime("%y%m%d")
    from_time = f"{day_str}{MARKET_OPEN}"
    to_time   = f"{day_str}{MARKET_CLOSE}"

    url     = "https://history.truedata.in/getbars"
    headers = {"Authorization": f"Bearer {token}"}
    params  = {"symbol": symbol, "from": from_time, "to": to_time, "interval": "1min", "response": "csv"}

    print(f"📥 Fetching {symbol} for {date.date()}...", end=" ")
    resp = requests.get(url, headers=headers, params=params)
    try:
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        if df.empty:
            print("⚠️ Empty")
            return None
        df["symbol"] = symbol
        df["date"]   = date.date()
        print("✅")
        return df
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def main():
    token   = authenticate()
    current = START_DATE

    while current <= END_DATE:
        symbol = get_symbol_by_date(current)
        df = fetch_day_data(token, symbol, current)
        if df is not None:
            all_data.append(df)
        current += timedelta(days=1)

    if all_data:
        df_all = pd.concat(all_data, ignore_index=True)
        df_all.to_csv(script_dir / OUTPUT_FILE, index=False)
        print(f"\n✅ All data saved to: {OUTPUT_FILE}")
    else:
        print("⚠️ No data fetched. Check date range or credentials.")

if __name__ == "__main__":
    main()
