"""
Deterministic Row ID Generator

Generates unique, deterministic IDs for market_data rows.
Format: {ticker}_{YYYYMMDD_HHmm}

🔒3: These IDs are the SOLE AUTHORITY for deduplication.
     Identical (ticker, window_start) inputs ALWAYS produce identical IDs.
     This enables restart-safe reconciliation against Google Sheets.
"""

from datetime import datetime


def generate_row_id(ticker: str, window_start: datetime) -> str:
    """
    Generate a deterministic unique row ID.

    Args:
        ticker: Instrument symbol, e.g., "NIFTY" or "BANKNIFTY"
        window_start: Window start datetime (IST-aware)

    Returns:
        Deterministic ID string, e.g., "NIFTY_20260221_0915"

    The same (ticker, window_start) pair will ALWAYS produce the same ID.
    Different pairs will ALWAYS produce different IDs.
    """
    ts_str = window_start.strftime("%Y%m%d_%H%M")
    return f"{ticker}_{ts_str}"


def parse_row_id(row_id: str) -> tuple:
    """
    Parse a row ID back into (ticker, window_start_str) components.

    Args:
        row_id: ID string, e.g., "NIFTY_20260221_0915"

    Returns:
        Tuple of (ticker, timestamp_str) — e.g., ("NIFTY", "20260221_0915")
    """
    # Split on last two underscores (ticker may contain underscores)
    parts = row_id.rsplit("_", 2)
    if len(parts) == 3:
        ticker = parts[0]
        ts_str = f"{parts[1]}_{parts[2]}"
        return ticker, ts_str
    raise ValueError(f"Invalid row ID format: {row_id}")
