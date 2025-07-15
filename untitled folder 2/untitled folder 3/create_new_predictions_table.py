import sqlite3
from db import DB_PATH

def list_tables():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cur.fetchall()
    print("📋 Tables in DB:", [t[0] for t in tables])
    conn.close()

def preview_new_predictions(limit=5):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT * FROM new_predictions LIMIT ?;", (limit,))
    rows = cur.fetchall()
    print(f"\n🔍 Preview of 'new_predictions' ({limit} rows):")
    for row in rows:
        print(row)
    conn.close()

if __name__ == "__main__":
    list_tables()
    preview_new_predictions()
