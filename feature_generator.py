"""
Incrementally compute and insert engineered features for new bars
into the SQLite 'features' table, avoiding pandas.Timestamp binding errors
by storing all datetime-like columns as "YYYY-MM-DD HH:MM:SS" strings.
All user-facing messages go through the 'console' logger so they
end up only in your terminal logs (not as raw prints).
"""

import logging
import pandas as pd
from trade_config import TradeConfig
from db import init_db, get_conn

# Console logger for user-visible messages
console = logging.getLogger("console")
# Module logger for internal info/debug
log = logging.getLogger(__name__)

# Feature-engineering imports
from training_features.price_action       import add_price_action_features
from training_features.volume_features    import add_volume_features
from training_features.volume_signals     import add_volume_features as add_volume_signals
from training_features.open_interest      import add_oi_features as add_open_interest_features
from training_features.structure_features import add_structure_features
from training_features.vwap_utils         import calculate_vwap as add_vwap_features
from training_features.hvn_engine         import compute_rolling_hvn as add_hvn_features
from training_features.time_features      import add_time_features
from training_features.momentum_features  import add_momentum_features
from training_features.trend_detector     import add_trend_regime as add_trend_label
from training_features.meta_features      import add_meta_quality_flags
from training_features.features_engineered import add_feature_engineering


def _compute_ema_filter_15(df_bars: pd.DataFrame) -> pd.Series:
    """
    Given raw 1-min bars with a 'timestamp' column, compute a 1-min
    EMA20/EMA50 regime flag and return a series of 1s and 0s aligned to each bar.

    This version prepends the last 50 bars from the 'bars' table for warm-up
    so that the EMA has the correct memory when running incrementally.
    """
    # Prepare datetime index
    df_temp = df_bars.copy()
    df_temp['datetime'] = pd.to_datetime(df_temp['timestamp'], errors='coerce')
    df_temp.set_index('datetime', inplace=True)

    # Extract close series
    close = df_temp['close']

    # Warm-up: pull prior 50 bars for EMA memory
    first_ts = df_temp.index[0]
    warmup_start_str = first_ts.strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        warmup_df = pd.read_sql(
            """
            SELECT timestamp, close
            FROM bars
            WHERE timestamp < ?
            ORDER BY timestamp DESC
            LIMIT 50
            """,
            conn,
            params=(warmup_start_str,)
        )

    if not warmup_df.empty:
        # Reverse to ascending order
        warmup_df = warmup_df.iloc[::-1].reset_index(drop=True)
        # Prepare combined close series
        warmup_closes = warmup_df['close']
        combined = pd.concat([
            warmup_closes,
            close.reset_index(drop=True)
        ], ignore_index=True)
        # Compute EMAs on combined history
        ema20 = combined.ewm(span=20, adjust=False).mean()
        ema50 = combined.ewm(span=50, adjust=False).mean()
        regime_all = (ema20 > ema50).astype(int)
        # Slice off warm-up portion
        regime_today = regime_all.iloc[len(warmup_closes):].reset_index(drop=True)
        # Return with original index alignment
        return pd.Series(regime_today.values, index=df_bars.index, dtype=int)

    # Fallback: full-day EWM if no warm-up data
    ema20 = close.ewm(span=20, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()
    regime = (ema20 > ema50).astype(int)
    ts = pd.to_datetime(df_bars['timestamp'], errors='coerce')
    return ts.map(regime).fillna(0).astype(int)


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the full historical feature pipeline to the incoming bars.
    Expects df to include 'timestamp','open','high','low','close','volume','open_interest',
    plus an 'ema_filter_15' boolean. Returns a DataFrame of engineered features.
    """
    # Align OI columns
    if 'oi' in df.columns and 'open_interest' not in df.columns:
        df['open_interest'] = df['oi']
    elif 'open_interest' in df.columns and 'oi' not in df.columns:
        df['oi'] = df['open_interest']

    # Ensure datetime column for sorting
    df['datetime'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df.sort_values('datetime', inplace=True)
    df.reset_index(drop=True, inplace=True)

    # ATR(14)
    df['prev_close'] = df['close'].shift(1)
    df['tr1']        = df['high'] - df['low']
    df['tr2']        = (df['high'] - df['prev_close']).abs()
    df['tr3']        = (df['low']  - df['prev_close']).abs()
    df['true_range'] = df[['tr1','tr2','tr3']].max(axis=1)
    df['atr']        = df['true_range'].rolling(window=14, min_periods=1).mean()
    df.drop(columns=['prev_close','tr1','tr2','tr3','true_range'], inplace=True)

    # Core pipeline
    df = add_price_action_features(df)
    df = add_volume_features(df)
    df = add_volume_signals(df)
    df = add_open_interest_features(df)
    df = add_structure_features(df)
    df = add_vwap_features(df)
    df = add_hvn_features(df)
    df = add_time_features(df)
    df = add_momentum_features(df)
    df = add_trend_label(df)
    df = add_meta_quality_flags(df)

    # Final flags & scores
    df = add_feature_engineering(df, str(TradeConfig.FEATURES_CSV))

    return df


def feature_generator_cycle() -> int:
    """
    1) Ensure DB & tables exist
    2) Read only new bars since last FEATURES timestamp
    3) Compute ema_filter_15, build features
    4) Convert datetime columns to strings
    5) INSERT OR IGNORE into 'features' table
    Returns number of rows inserted.
    """
    init_db()

    # Ensure 'ema_filter_15' column exists in features table
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(features)")
        columns = [row[1] for row in cur.fetchall()]
        if 'ema_filter_15' not in columns:
            cur.execute("ALTER TABLE features ADD COLUMN ema_filter_15 INTEGER DEFAULT 0;")
            conn.commit()
            console.info("✅ Added missing 'ema_filter_15' column to features table")

    # 1) Fetch last feature timestamp
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT MAX(timestamp) FROM features")
        row = cur.fetchone()
        last_ts = row[0] if row and row[0] is not None else None

        # 2) Load new bars
        if last_ts:
            df_bars = pd.read_sql(
                "SELECT * FROM bars WHERE timestamp > ? ORDER BY timestamp",
                conn,
                params=(last_ts,)
            )
        else:
            df_bars = pd.read_sql(
                "SELECT * FROM bars ORDER BY timestamp",
                conn
            )

    if df_bars.empty:
        log.info("No new bars to feature.")
        return 0

    # 3) Compute EMA regime filter
    ema_flag_series = _compute_ema_filter_15(df_bars)
    df_bars['ema_filter_15'] = ema_flag_series

    # 4) Build all other features
    df_feat = build_features(df_bars)

    # 5) Ensure ema_filter_15 carried into final df
    if 'ema_filter_15' not in df_feat.columns:
        df_feat['ema_filter_15'] = ema_flag_series.values

    # 6) Convert timestamps to plain strings
    df_feat['timestamp'] = (
        pd.to_datetime(df_feat['timestamp'], errors='coerce')
          .dt.strftime("%Y-%m-%d %H:%M:%S")
    )
    df_feat['datetime'] = (
        pd.to_datetime(df_feat['datetime'], errors='coerce')
          .dt.strftime("%Y-%m-%d %H:%M:%S")
    )
    if 'date' in df_feat.columns:
        df_feat['date'] = df_feat['date'].astype(str)

    # 7) Insert into SQLite
    cols = df_feat.columns.tolist()
    placeholders = ",".join("?" for _ in cols)
    sql = f"INSERT OR IGNORE INTO features ({','.join(cols)}) VALUES ({placeholders})"

    inserted = 0
    with get_conn() as conn:
        cur = conn.cursor()
        for row in df_feat.itertuples(index=False, name=None):
            try:
                cur.execute(sql, row)
                inserted += cur.rowcount
            except Exception as e:
                log.error("Feature insert failed: %s", e)
        conn.commit()

    log.info(
        "Inserted %d new feature rows into SQLite 'features' table",
        inserted
    )

    console.info(f"✅ Inserted {inserted} feature rows")
    return inserted


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    count = feature_generator_cycle()
    console.info(f"✅ Inserted {count} feature rows")
