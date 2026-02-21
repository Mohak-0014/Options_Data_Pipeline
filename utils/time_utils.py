"""
IST Time Utilities — Boundary Computation, Window Assignment, Market Hours

All time operations use IST (Asia/Kolkata) and the pre-computed boundary list.
No modulo arithmetic is used anywhere per spec requirement.

🔒1: assign_tick_to_window() maps exchange timestamps to their owning window.
"""

from datetime import date, datetime, time, timedelta
from typing import List, Optional, Tuple

from config.settings import (
    CANDLE_INTERVAL_MINUTES,
    IST,
    MARKET_CLOSE,
    MARKET_OPEN,
)


def get_current_ist() -> datetime:
    """Return the current datetime in IST timezone."""
    return datetime.now(tz=IST)


def _generate_boundary_list(
    session_open: time,
    session_close: time,
    target_date: Optional[date] = None,
) -> List[datetime]:
    """
    Generate the complete list of 5-minute window START times for a session.

    The first window starts at session_open (e.g., 09:15).
    Boundaries are: open, open+5, open+10, ..., up to (but not exceeding) close.

    Returns datetime objects with IST timezone for the given date.
    """
    if target_date is None:
        target_date = get_current_ist().date()

    boundaries = []
    current = datetime.combine(target_date, session_open, tzinfo=IST)
    end = datetime.combine(target_date, session_close, tzinfo=IST)
    interval = timedelta(minutes=CANDLE_INTERVAL_MINUTES)

    while current < end:
        boundaries.append(current)
        current += interval

    return boundaries


def generate_all_windows(
    target_date: Optional[date] = None,
    session_open: Optional[time] = None,
    session_close: Optional[time] = None,
) -> List[datetime]:
    """
    Generate all valid 5-minute window START times for a trading day.

    For default session (09:15–15:30), returns:
    [09:15, 09:20, 09:25, ..., 15:25]
    That's 75 windows.

    For special sessions, uses the provided open/close times.
    """
    open_time = session_open or MARKET_OPEN
    close_time = session_close or MARKET_CLOSE
    return _generate_boundary_list(open_time, close_time, target_date)


def generate_finalization_times(
    target_date: Optional[date] = None,
    session_open: Optional[time] = None,
    session_close: Optional[time] = None,
) -> List[datetime]:
    """
    Generate all boundary times at which candles should be finalized.

    For default session, returns:
    [09:20, 09:25, ..., 15:30]
    That's 75 finalization points.

    Each finalization time is window_start + 5 minutes.
    """
    windows = generate_all_windows(target_date, session_open, session_close)
    interval = timedelta(minutes=CANDLE_INTERVAL_MINUTES)
    return [w + interval for w in windows]


def get_current_window_start(dt: Optional[datetime] = None) -> datetime:
    """
    Floor the given datetime to the nearest 5-minute window start boundary.

    Uses the pre-computed boundary list — no modulo.
    Returns the most recent boundary that is <= dt.
    """
    if dt is None:
        dt = get_current_ist()

    # Ensure IST
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)

    boundaries = _generate_boundary_list(MARKET_OPEN, MARKET_CLOSE, dt.date())

    # Find the most recent boundary <= dt
    result = None
    for boundary in boundaries:
        if boundary <= dt:
            result = boundary
        else:
            break

    return result


def get_next_window_boundary(dt: Optional[datetime] = None) -> Optional[datetime]:
    """
    Returns the next 5-minute boundary AFTER the given datetime.

    If dt is past the last boundary, returns None (session over).
    """
    if dt is None:
        dt = get_current_ist()

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)

    finalization_times = generate_finalization_times(target_date=dt.date())

    for boundary in finalization_times:
        if boundary > dt:
            return boundary

    return None  # Past session close


def is_market_hours(
    dt: Optional[datetime] = None,
    session_open: Optional[time] = None,
    session_close: Optional[time] = None,
) -> bool:
    """
    Returns True if the datetime falls within market session hours.

    Uses provided session times or defaults to MARKET_OPEN/MARKET_CLOSE.
    Inclusive of open, exclusive of close (09:15:00 ≤ t < 15:30:00).
    """
    if dt is None:
        dt = get_current_ist()

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)

    open_time = session_open or MARKET_OPEN
    close_time = session_close or MARKET_CLOSE

    current_time = dt.timetz()

    # Handle the time comparison (ignoring tzinfo for simplicity)
    dt_time = dt.time()
    return open_time <= dt_time < close_time


def assign_tick_to_window(exchange_timestamp: datetime) -> datetime:
    """
    🔒1 Deterministic mapping from exchange timestamp to its owning window start.

    This is the ONLY function that should be used for tick-to-window assignment.
    The exchange timestamp from the tick payload is the sole source of truth.

    Rules:
    - A tick at exactly 09:20:00.000 belongs to window starting at 09:20
    - A tick at 09:19:59.999 belongs to window starting at 09:15
    - Uses the pre-computed boundary list, no modulo

    Returns: The window_start datetime that owns this tick.

    Raises ValueError if the exchange timestamp is outside market hours.
    """
    if exchange_timestamp.tzinfo is None:
        exchange_timestamp = exchange_timestamp.replace(tzinfo=IST)

    boundaries = _generate_boundary_list(
        MARKET_OPEN, MARKET_CLOSE, exchange_timestamp.date()
    )

    if not boundaries:
        raise ValueError(
            f"No trading boundaries for date {exchange_timestamp.date()}"
        )

    # Check if before market open
    if exchange_timestamp < boundaries[0]:
        raise ValueError(
            f"Exchange timestamp {exchange_timestamp} is before market open "
            f"{boundaries[0]}"
        )

    # Find the window: the largest boundary that is <= exchange_timestamp
    owning_window = None
    for boundary in boundaries:
        if boundary <= exchange_timestamp:
            owning_window = boundary
        else:
            break

    if owning_window is None:
        raise ValueError(
            f"Could not assign window for exchange timestamp {exchange_timestamp}"
        )

    # Check if after last window's end (session close)
    interval = timedelta(minutes=CANDLE_INTERVAL_MINUTES)
    last_window_end = boundaries[-1] + interval
    if exchange_timestamp >= last_window_end:
        raise ValueError(
            f"Exchange timestamp {exchange_timestamp} is after session close "
            f"{last_window_end}"
        )

    return owning_window
