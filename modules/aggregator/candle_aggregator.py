"""
Candle Aggregator — IST-Aligned 5-Minute Window Finalization

Manages the window lifecycle state machine and coordinates between
the tick buffer and the ATR engine.

🔒2: Window freeze state machine (COLLECTING → FREEZING → FROZEN → COLLECTING)
     prevents cross-window contamination.
🔒4: Uses trading calendar for session-aware boundary generation.
"""

import enum
import threading
import time as time_module
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional, Tuple

from config.settings import (
    CANDLE_INTERVAL_MINUTES,
    IST,
    TICKER_COUNT,
    WINDOW_FREEZE_MS,
)
from modules.aggregator.tick_buffer import OHLCCandle, TickBuffer
from utils.logger import get_logger

logger = get_logger("aggregator.candle_aggregator")


class WindowState(enum.Enum):
    """🔒2 Window lifecycle states."""
    COLLECTING = "COLLECTING"   # Accepting ticks for current window
    FREEZING = "FREEZING"       # Boundary reached, waiting for in-flight ticks
    FROZEN = "FROZEN"           # Snapshot taken, no more updates
    IDLE = "IDLE"               # Outside market hours


class CandleAggregator:
    """
    Manages IST-aligned 5-minute candle windows.

    Responsibilities:
    - Pre-compute valid window boundaries (no modulo arithmetic)
    - Manage window lifecycle state machine
    - Coordinate tick buffer freeze → snapshot → reset
    - Validate finalized candles (178 tickers, OHLC invariants)
    """

    def __init__(self, tick_buffer: TickBuffer):
        self._tick_buffer = tick_buffer
        self._state = WindowState.IDLE
        self._state_lock = threading.Lock()
        self._current_window: Optional[datetime] = None
        self._boundaries: List[datetime] = []
        self._finalization_times: List[datetime] = []
        self._boundary_index: int = 0

    @property
    def state(self) -> WindowState:
        return self._state

    @property
    def current_window(self) -> Optional[datetime]:
        return self._current_window

    def initialize_for_session(
        self,
        target_date: Optional[date] = None,
        session_open: Optional[time] = None,
        session_close: Optional[time] = None,
    ) -> None:
        """
        Pre-compute all window boundaries for the trading session.

        🔒4: Uses provided session hours (from trading calendar) or defaults.
        """
        from utils.time_utils import generate_all_windows, generate_finalization_times

        self._boundaries = generate_all_windows(
            target_date, session_open, session_close
        )
        self._finalization_times = generate_finalization_times(
            target_date, session_open, session_close
        )
        self._boundary_index = 0

        logger.info(
            f"SESSION_INIT | date={target_date or 'today'} | "
            f"windows={len(self._boundaries)} | "
            f"first={self._boundaries[0].time() if self._boundaries else 'N/A'} | "
            f"last={self._boundaries[-1].time() if self._boundaries else 'N/A'}"
        )

    def start_window(self, window_start: datetime) -> None:
        """
        Begin collecting ticks for a new window.

        Sets state to COLLECTING and configures the tick buffer.
        """
        with self._state_lock:
            self._current_window = window_start
            self._state = WindowState.COLLECTING
            self._tick_buffer.set_active_window(window_start)

        logger.info(f"WINDOW_START | window={window_start.time()}")

    def begin_freeze(self) -> None:
        """
        🔒2 Begin the freeze period.

        This is called at the boundary crossing (e.g., at 09:20:00.000).
        The buffer stops accepting ticks immediately via freeze().
        After WINDOW_FREEZE_MS, finalize() should be called.
        """
        with self._state_lock:
            if self._state != WindowState.COLLECTING:
                logger.warning(
                    f"FREEZE_SKIP | unexpected state={self._state.value} | "
                    f"window={self._current_window}"
                )
                return
            self._state = WindowState.FREEZING

        # Freeze the tick buffer
        self._tick_buffer.freeze()

        logger.debug(
            f"FREEZE_BEGIN | window={self._current_window.time() if self._current_window else 'N/A'} | "
            f"freeze_ms={WINDOW_FREEZE_MS}"
        )

    def finalize_window(self) -> Tuple[Optional[datetime], Dict[str, OHLCCandle]]:
        """
        🔒2 Finalize the current window after freeze period.

        Steps:
        1. Set state to FROZEN
        2. Snapshot and reset the tick buffer
        3. Validate candle data
        4. Return (window_start, candles)

        Returns:
            Tuple of (window_start, Dict[ticker, OHLCCandle])
            Returns (None, {}) if no window was active.
        """
        with self._state_lock:
            if self._state not in (WindowState.FREEZING, WindowState.COLLECTING):
                logger.warning(
                    f"FINALIZE_SKIP | state={self._state.value}"
                )
                return None, {}
            self._state = WindowState.FROZEN
            window = self._current_window

        # Snapshot (guaranteed atomic by tick_buffer's lock)
        candles = self._tick_buffer.snapshot_and_reset()

        # Validate
        self._validate_candles(window, candles)

        logger.info(
            f"WINDOW_FINALIZED | window={window.time() if window else 'N/A'} | "
            f"tickers={len(candles)}/{TICKER_COUNT}"
        )

        return window, candles

    def transition_to_next_window(self, next_window: datetime) -> None:
        """
        Transition from FROZEN to COLLECTING for the next window.
        """
        with self._state_lock:
            self._state = WindowState.COLLECTING
            self._current_window = next_window
            self._tick_buffer.set_active_window(next_window)

        logger.debug(f"NEXT_WINDOW | window={next_window.time()}")

    def set_idle(self) -> None:
        """Set state to IDLE (outside market hours)."""
        with self._state_lock:
            self._state = WindowState.IDLE

    def _validate_candles(
        self, window: Optional[datetime], candles: Dict[str, OHLCCandle]
    ) -> None:
        """
        Validate finalized candle batch:
        - Check 178 tickers present
        - Verify OHLC invariants (high >= max(open,close), low <= min(open,close))
        - Log warnings for any issues
        """
        # Missing ticker check
        if len(candles) < TICKER_COUNT:
            from config.instruments import get_all_symbols
            expected = set(get_all_symbols())
            present = set(candles.keys())
            missing = expected - present
            logger.warning(
                f"MISSING_TICKERS | window={window} | "
                f"expected={TICKER_COUNT} | present={len(candles)} | "
                f"missing={sorted(missing)}"
            )

        # OHLC invariant check
        for ticker, candle in candles.items():
            if candle.high < max(candle.open, candle.close):
                logger.warning(
                    f"OHLC_INVARIANT | ticker={ticker} | "
                    f"high={candle.high} < max(open={candle.open}, close={candle.close})"
                )
            if candle.low > min(candle.open, candle.close):
                logger.warning(
                    f"OHLC_INVARIANT | ticker={ticker} | "
                    f"low={candle.low} > min(open={candle.open}, close={candle.close})"
                )

    def get_finalization_schedule(self) -> List[datetime]:
        """Return list of all finalization times for the session."""
        return list(self._finalization_times)

    def get_window_boundaries(self) -> List[datetime]:
        """Return list of all window start times for the session."""
        return list(self._boundaries)
