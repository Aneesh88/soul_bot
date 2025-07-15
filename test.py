import sqlite3
import os

# ✅ Update this path if needed
DB_PATH = "/Users/aneeshviswanathan/Desktop/Om_Babaji_Om_threshold/core_files/trading_data.db"

def clear_tables():
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found at: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    try:
        cur.execute("DELETE FROM live_trade_details")
        cur.execute("DELETE FROM daily_trade_state")
        conn.commit()
        print("✅ Cleared both 'live_trade_details' and 'daily_trade_state' tables.")
    except Exception as e:
        print(f"❌ Error clearing tables: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    clear_tables()
