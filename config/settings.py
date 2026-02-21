"""
Kotak Neo 5-Minute Volatility Harvester — Central Configuration

Loads environment variables and defines all system-wide constants.
This module is the single source of truth for all configuration values.
"""

import os
from datetime import time
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
load_dotenv()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = Path(os.getenv("LOG_DIR", str(PROJECT_ROOT / "logs")))
CHECKPOINT_DIR = Path(os.getenv("CHECKPOINT_DIR", str(PROJECT_ROOT / "data" / "checkpoints")))
FALLBACK_DIR = Path(os.getenv("FALLBACK_DIR", str(PROJECT_ROOT / "data" / "fallback")))
CALENDAR_DIR = PROJECT_ROOT / "data" / "calendars"

# ---------------------------------------------------------------------------
# Timezone
# ---------------------------------------------------------------------------
IST = ZoneInfo("Asia/Kolkata")

# ---------------------------------------------------------------------------
# Market Session — Default (overridden by trading_calendar for special sessions)
# ---------------------------------------------------------------------------
MARKET_OPEN = time(9, 15)    # 09:15 IST
MARKET_CLOSE = time(15, 30)  # 15:30 IST

# ---------------------------------------------------------------------------
# Candle Configuration
# ---------------------------------------------------------------------------
CANDLE_INTERVAL_MINUTES = 5
ATR_PERIOD = 14
ATR_PRECISION = 4  # Decimal places for ATR rounding
TICKER_COUNT = 178

# ---------------------------------------------------------------------------
# Write Pipeline
# ---------------------------------------------------------------------------
MAX_RETRIES = 5
RETRY_BASE_DELAY_S = 1  # Exponential backoff: 1s, 2s, 4s, 8s, 16s
WRITE_TIMEOUT_S = 30     # NFR: write completion < 30 seconds

# ---------------------------------------------------------------------------
# 🔒2 Window Freeze Configuration
# ---------------------------------------------------------------------------
WINDOW_FREEZE_MS = 500      # Grace period (ms) after boundary before snapshot
LATE_TICK_TOLERANCE_MS = 200  # Max age (ms) for a late tick to be accepted during freeze

# ---------------------------------------------------------------------------
# 🔒7 Callback Latency Monitoring
# ---------------------------------------------------------------------------
CALLBACK_LATENCY_WARN_US = 500   # p99 warning threshold in microseconds
CALLBACK_LATENCY_MAX_US = 2000   # Hard max threshold
LATENCY_SAMPLE_SIZE = 10_000     # Rolling window for latency samples

# ---------------------------------------------------------------------------
# WebSocket & Heartbeat
# ---------------------------------------------------------------------------
WS_SUBSCRIBE_BATCH_SIZE = 50
HEARTBEAT_SILENCE_TIMEOUT_S = 30  # Trigger reconnect if no tick for 30s
SESSION_MAX_AGE_HOURS = 12        # Re-authenticate after 12 hours

# ---------------------------------------------------------------------------
# Kotak Neo API Credentials (from .env)
# Kotak Neo v2 SDK uses a single consumer_key (token from Neo app/web)
# Auth flow: totp_login(mobile, ucc, totp) → totp_validate(mpin)
# ---------------------------------------------------------------------------
KOTAK_CONSUMER_KEY = os.getenv("KOTAK_CONSUMER_KEY", "")  # Token from Neo app → Invest → Trade API
KOTAK_MOBILE = os.getenv("KOTAK_MOBILE", "")              # Registered mobile with country code
KOTAK_UCC = os.getenv("KOTAK_UCC", "")                    # Unique Client Code (profile section)
KOTAK_MPIN = os.getenv("KOTAK_MPIN", "")                  # Neo account MPIN
TOTP_SECRET = os.getenv("TOTP_SECRET", "")                # Base32 TOTP secret for authenticator

# ---------------------------------------------------------------------------
# Google Sheets (from .env)
# ---------------------------------------------------------------------------
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------
MAX_CHECKPOINT_FILES = 3  # Keep last N checkpoint files for safety
