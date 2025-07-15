import sqlite3

DB_PATH = "/Users/aneeshviswanathan/Desktop/Om_Babaji_Om_threshold/core_files/trading_data.db"

def show_table_columns(table_name):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table_name});")
        columns = cur.fetchall()
        print(f"\n🔍 Columns in '{table_name}':")
        for col in columns:
            print(f"  {col[1]} ({col[2]})")  # column name and type

if __name__ == "__main__":
    show_table_columns("live_trade_details")
    show_table_columns("daily_trade_state")
