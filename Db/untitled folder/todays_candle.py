import requests
from datetime import datetime, date
from pathlib import Path
import pandas as pd
import io

# ────── USER CONFIG ───────────────────────
USERNAME     = "tdwsp674"
PASSWORD     = "aneesh@674"

# Market session times
MARKET_OPEN  = "T09:15:00"
MARKET_CLOSE = "T15:30:00"

# Output directory (script folder)
script_dir = Path(__file__).parent

# Expiry dates mapping (same as before)
expiry_dates = [
    (datetime.strptime(d, "%d/%m/%Y"), f"BANKNIFTY{datetime.strptime(d, '%d/%m/%Y').strftime('%y%b').upper()}FUT")
    for d in [
        "25/01/2024", "29/02/2024", "27/03/2024", "24/04/2024", "29/05/2024", "26/06/2024",
        "31/07/2024", "28/08/2024", "25/09/2024", "30/10/2024", "27/11/2024", "24/12/2024",
        "30/01/2025", "27/02/2025", "27/03/2025", "24/04/2025", "29/05/2025", "26/06/2025"
    ]
]

def authenticate():
    auth_url = "https://auth.truedata.in/token"
    payload = {"username": USERNAME, "password": PASSWORD, "grant_type": "password"}
    resp = requests.post(auth_url, data=payload)
    resp.raise_for_status()
    return resp.json().get("access_token")


def get_symbol_by_date(target: date) -> str:
    for expiry, symbol in expiry_dates:
        if target <= expiry.date():
            return symbol
    raise ValueError(f"No future contract defined for date {target}")


def fetch_day_data(token: str, symbol: str, target: date) -> pd.DataFrame:
    day_str   = target.strftime("%y%m%d")
    from_time = f"{day_str}{MARKET_OPEN}"
    to_time   = f"{day_str}{MARKET_CLOSE}"

    url     = "https://history.truedata.in/getbars"
    headers = {"Authorization": f"Bearer {token}"}
    params  = {"symbol": symbol, "from": from_time, "to": to_time, "interval": "1min", "response": "csv"}

    print(f"📥 Fetching {symbol} for {target}...", end=" ")
    resp = requests.get(url, headers=headers, params=params)
    try:
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        if df.empty:
            print("⚠️ Empty data returned")
            return None
        # Append metadata
        df["symbol"] = symbol
        df["date"]   = target
        print(f"✅ Received {len(df)} rows")
        return df
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def main():
    # 1) Authenticate once
    token = authenticate()

    # 2) Determine today
    today = date.today()
    symbol = get_symbol_by_date(today)

    # 3) Fetch today's data
    df_today = fetch_day_data(token, symbol, today)
    if df_today is None:
        print(f"⚠️ No data fetched for {today}")
        return

    # 4) Save to CSV
    out_file = script_dir / f"bars_{today}.csv"
    df_today.to_csv(out_file, index=False)
    print(f"\n✅ Export complete: {out_file}")

if __name__ == "__main__":
    main()
