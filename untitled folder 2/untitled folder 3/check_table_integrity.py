import sqlite3
import pandas as pd
import os
from datetime import datetime
from db import get_conn  # Use actual DB connection

# ✅ Only check these 3 core tables
TABLES_TO_CHECK = {
    "bars": "timestamp",
    "features": "timestamp",
    "new_predictions": "timestamp"
}

START_DATE = "2024-01-01"

def generate_full_timestamp_range(start_date, end_datetime, freq):
    return pd.date_range(start=start_date, end=end_datetime, freq=freq)

def check_table_integrity(cursor, table_name, time_col):
    cursor.execute(f"SELECT {time_col} FROM {table_name}")
    rows = cursor.fetchall()
    if not rows:
        return f"\n❌ {table_name}: No data found.\n"

    timestamps = [r[0] for r in rows]
    try:
        df = pd.to_datetime(pd.Series(timestamps))
    except Exception as e:
        return f"\n❌ {table_name}: Failed to parse timestamps: {e}\n"

    df_sorted = df.sort_values()
    duplicates = df_sorted[df_sorted.duplicated()].tolist()
    unique_timestamps = df_sorted.drop_duplicates()
    end_time = pd.to_datetime(datetime.now())

    freq = "1D" if table_name == "new_predictions" else "1min"
    full_range = generate_full_timestamp_range(START_DATE, end_time, freq=freq)
    missing = full_range.difference(unique_timestamps)

    report = f"\n✅ Checking table: {table_name}\n"
    report += f"  ➤ Total entries        : {len(df)}\n"
    report += f"  ➤ Duplicates found     : {len(duplicates)}\n"
    report += f"  ➤ Missing timestamps   : {len(missing)}\n"

    if duplicates:
        report += "\n  🟡 Duplicate timestamps:\n"
        report += "\n".join([f"    - {d}" for d in duplicates[:10]])
        if len(duplicates) > 10:
            report += f"\n    ...and {len(duplicates) - 10} more"

    if len(missing) > 0:
        report += "\n\n  🔴 Missing timestamps:\n"
        report += "\n".join([f"    - {m}" for m in missing[:10]])
        if len(missing) > 10:
            report += f"\n    ...and {len(missing) - 10} more"

    report += "\n" + "-" * 60
    return report

def main():
    try:
        conn = get_conn()
        cursor = conn.cursor()
    except Exception as e:
        print(f"❌ Failed to connect to DB: {e}")
        return

    full_report = f"🧾 INTEGRITY CHECK REPORT — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    full_report += "=" * 60

    for table, time_col in TABLES_TO_CHECK.items():
        try:
            section_report = check_table_integrity(cursor, table, time_col)
        except Exception as e:
            section_report = f"\n❌ Error checking table {table}: {e}\n"
        full_report += section_report

    # Save report inside project/state/
    project_dir = os.path.dirname(os.path.abspath(__file__))
    state_dir = os.path.join(project_dir, "state")
    os.makedirs(state_dir, exist_ok=True)

    report_path = os.path.join(state_dir, "integrity_report.txt")
    with open(report_path, "w") as f:
        f.write(full_report)

    print(full_report)
    print(f"\n✅ Report saved to {report_path}")

if __name__ == "__main__":
    main()
