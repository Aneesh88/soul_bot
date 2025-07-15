# trade_config.py
from dataclasses import dataclass
from pathlib import Path

# Project root = this file’s directory (this is module-level, accessible from anywhere)
BASE_DIR = Path(__file__).resolve().parent

@dataclass(frozen=True)
class TradeConfig:
    # --- Core data files and model ---
    CORE_CSV:     Path = BASE_DIR / "core_files" / "JAN2024_TO_JUN2025_BANKNIFTY_FUT.csv"
    FEATURES_CSV: Path = BASE_DIR / "core_files" / "EVAL_features_final.csv"
    PRED_CSV:     Path = BASE_DIR / "core_files" / "model_predictions.csv"
    MODEL_PKL:    Path = BASE_DIR / "models"       / "xgb_model.pkl"

    # --- Archival files (JSON & CSV) ---
    CLOSED_TRADES_JSON: Path = BASE_DIR / "logs" / "closed_trades.json"
    TRADE_HISTORY_CSV:  Path = BASE_DIR / "logs" / "trade_history.csv"

    # --- Trading time windows & thresholds (IST) ---
    ENTRY_START: str   = "09:20"
    ENTRY_END:   str   = "14:25"
    FORCED_EXIT: str   = "15:13"
    LONG_TH:     float = 0.85
    SHORT_TH:    float = 0.85

    # --- Entry/Exit smoothing controls ---
    ENABLE_ENTRY_SMOOTHING:  bool = True
    ENABLE_EXIT_SMOOTHING:   bool = True
    WEIGHTED_ENTRY_SMOOTHING: bool = True
    WEIGHTED_EXIT_SMOOTHING:  bool = True
    ENTRY_SMOOTHING_WINDOW: int = 3
    EXIT_SMOOTHING_WINDOW:  int = 15

    # --- Risk management settings ---
    FIXED_TP: int = 50
    FIXED_SL: int = 160
    MAX_TRADES_PER_DAY: int = 70
    MAX_CONCURRENT_TRADES: int = 60
    EXIT_SHORT_CONF: float = 0.51
    EXIT_LONG_CONF:  float = 0.51

    # --- Raw bar CSV column order ---
    BAR_COLS: tuple = (
        "timestamp", "open", "high", "low", "close", "volume", "open_interest"
    )

    # --- TrueData API credentials ---
    TD_USER:             str = "tdwsp674"
    TD_PASS:             str = "aneesh@674"
    TD_SYMBOL:           str = "BANKNIFTY25JULFUT"
    TD_INTERVAL:         str = "1min"
    TD_EXPIRY:           str = "2025-07-31"
    TRUE_DATA_MAX_RETRIES: int = 3
    TRUE_DATA_RETRY_DELAY:  int = 5

    # --- ICICI Breeze API credentials ---
    BREEZE_KEY:    str = "55aW3D7Q29_8124(1Z2640_x66I82@02"
    BREEZE_SECRET: str = "u6385U69mpR341E29@0882255wO851S1"
    BREEZE_TOKEN:  str = "52218364"
    BREEZE_EXPIRY: str = "2025-07-31T06:00:00.000Z"

    # --- ATM and order sizing ---
    ATM_STRIKE_ROUNDING: int = 100
    ORDER_QUANTITY:      int = 35

    # --- Telegram notifications ---
    TELEGRAM_TOKEN:   str = "7325593988:AAGEeA3L01rmfHqFZ38cF070G1WOTkv-8JU"
    TELEGRAM_CHAT_ID: str = "6468675993"

    # --- EMA filter used only for logging ---
    EMA_FILTER_COLUMN: str = "ema_filter_15"

    USE_SQUARE_OFF: bool = True

    
    # trade_config.py
    ENTRY_MAX_SIGNAL_AGE: int = 120  # seconds

