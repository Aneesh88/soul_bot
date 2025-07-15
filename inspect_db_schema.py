# inspect_db_schema.py

import sqlite3
from pathlib import Path

# Adjust path if needed
DB_PATH = Path(__file__).resolve().parent / "core_files" / "trading_data.db"

def inspect_all_tables():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Fetch all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]

    print(f"📋 Tables in DB ({DB_PATH.name}):")
    for table in tables:
        print(f"\n🔹 Table: {table}")
        cursor.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        for col in columns:
            cid, name, dtype, notnull, dflt, pk = col
            print(f"    {name} ({dtype}) {'[PK]' if pk else ''}")

    conn.close()

if __name__ == "__main__":
    inspect_all_tables()
