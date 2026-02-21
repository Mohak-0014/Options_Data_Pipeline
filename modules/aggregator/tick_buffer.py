"""
Thread-Safe Tick Buffer — In-Memory OHLC Accumulator

Collects ticks and builds OHLC candles per ticker for the current window.
All access is protected by a threading.Lock for safe concurrent use
between the WebSocket listener thread and the scheduler thread.

🔒1: Window assignment uses exchange timestamps (not system clock).
🔒2: Late-tick and future-tick detection prevents cross-window contamination.
"""

import threading
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional

from config.settings import IST
from utils.logger import get_logger

logger = get_logger("aggregator.tick_buffer")


@dataclass
class OHLCCandle:
    """OHLC candle data for a single ticker in a single window."""
    window_start: datetime
    open: float
    high: float
    low: float
    close: float
    tick_count: int = 0

    def to_dict(self) -> dict:
        return {
            "window_start": self.window_start.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "tick_count": self.tick_count,
        }


class TickBuffer:
    """
    Thread-safe in-memory OHLC candle accumulator.

    The buffer holds one candle per ticker for the current active window.
    Memory is bounded: only current-window data (178 entries × ~5 floats).

    Thread safety:
    - All public methods acquire self._lock before mutating state.
    - snapshot_and_reset() performs a deep copy + reset atomically.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._buffer: Dict[str, OHLCCandle] = {}
        self._active_window: Optional[datetime] = None
        self._late_tick_count: int = 0
        self._future_tick_count: int = 0
        self._frozen: bool = False

    @property
    def active_window(self) -> Optional[datetime]:
        """Current active window start (read-only)."""
        return self._active_window

    @property
    def is_frozen(self) -> bool:
        """Whether the buffer is currently frozen (no more updates accepted)."""
        return self._frozen

    def set_active_window(self, window_start: datetime) -> None:
        """
        Set the active window for the buffer.
        Called by the aggregator when a new window begins (after freeze completes).
        """
        with self._lock:
            self._active_window = window_start
            self._frozen = False
            self._late_tick_count = 0
            self._future_tick_count = 0

    def freeze(self) -> None:
        """
        🔒2 Freeze the buffer — no more updates accepted.
        Called by the aggregator at start of freeze period.
        """
        with self._lock:
            self._frozen = True

    def update(self, ticker: str, price: float, window_start: datetime) -> bool:
        """
        Update OHLC candle for a ticker with a new tick.

        🔒1: window_start must be derived from exchange timestamp, not system clock.
        🔒2: Late ticks (for already-frozen windows) are dropped and logged.

        Args:
            ticker: Instrument symbol
            price: Last traded price (LTP)
            window_start: Window start time (from assign_tick_to_window)

        Returns:
            True if tick was accepted, False if dropped (late/future/frozen)
        """
        with self._lock:
            # 🔒2: Check if buffer is frozen
            if self._frozen:
                self._late_tick_count += 1
                # Note: do NOT log inside lock in hot path — just count
                return False

            # 🔒2: Check for late tick (belongs to a past window)
            if self._active_window is not None and window_start < self._active_window:
                self._late_tick_count += 1
                return False

            # 🔒2: Check for future tick (belongs to a future window)
            if self._active_window is not None and window_start > self._active_window:
                self._future_tick_count += 1
                return False

            # Normal update
            if ticker not in self._buffer:
                # First tick for this ticker in this window
                self._buffer[ticker] = OHLCCandle(
                    window_start=window_start,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    tick_count=1,
                )
            else:
                candle = self._buffer[ticker]
                candle.high = max(candle.high, price)
                candle.low = min(candle.low, price)
                candle.close = price
                candle.tick_count += 1

            return True

    def snapshot_and_reset(self) -> Dict[str, OHLCCandle]:
        """
        Atomically extract current buffer state and reset.

        This is the ONLY way to read finalized candle data.
        Returns a deep copy — the caller owns the returned data.

        Also logs late/future tick counts for observability.
        """
        with self._lock:
            snapshot = deepcopy(self._buffer)
            window = self._active_window
            late_count = self._late_tick_count
            future_count = self._future_tick_count

            # Reset
            self._buffer.clear()
            self._late_tick_count = 0
            self._future_tick_count = 0
            # Note: frozen state and active_window are NOT reset here
            # — the aggregator manages those transitions

        # Log outside lock
        if late_count > 0:
            logger.warning(
                f"LATE_TICKS_DROPPED | window={window} | count={late_count}"
            )
        if future_count > 0:
            logger.warning(
                f"FUTURE_TICKS_DROPPED | window={window} | count={future_count}"
            )

        logger.info(
            f"SNAPSHOT | window={window} | tickers={len(snapshot)} | "
            f"late_dropped={late_count} | future_dropped={future_count}"
        )

        return snapshot

    def get_ticker_count(self) -> int:
        """Return number of tickers currently in the buffer."""
        with self._lock:
            return len(self._buffer)

    def get_stats(self) -> dict:
        """Return current buffer statistics (for monitoring)."""
        with self._lock:
            return {
                "active_window": self._active_window.isoformat() if self._active_window else None,
                "ticker_count": len(self._buffer),
                "frozen": self._frozen,
                "late_tick_count": self._late_tick_count,
                "future_tick_count": self._future_tick_count,
                "total_ticks": sum(c.tick_count for c in self._buffer.values()),
            }
