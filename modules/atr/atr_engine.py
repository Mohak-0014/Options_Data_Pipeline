"""
ATR Engine — 14-Period Average True Range Calculator

Computes True Range and ATR using Wilder's recursive smoothing formula.
Maintains per-ticker state for continuous operation across windows.
Supports cold-start warmup and state serialization for checkpoint/recovery.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from config.settings import ATR_PERIOD, ATR_PRECISION
from modules.aggregator.tick_buffer import OHLCCandle
from utils.logger import get_logger

logger = get_logger("atr.atr_engine")


@dataclass
class ATRState:
    """Per-ticker ATR computation state."""
    prev_close: Optional[float] = None
    prev_atr: Optional[float] = None
    tr_history: List[float] = field(default_factory=list)  # Only during warmup
    candle_count: int = 0  # Total candles processed


@dataclass
class EnrichedCandle:
    """OHLC candle enriched with TR and ATR values."""
    ticker: str
    window_start: datetime
    open: float
    high: float
    low: float
    close: float
    tr: Optional[float]
    atr: Optional[float]
    tick_count: int
    gap_filled: bool = False

    def to_row(self, row_id: str, segment: str = "") -> list:
        """Format as a Google Sheets row."""
        from utils.time_utils import get_current_ist
        return [
            row_id,
            self.window_start.isoformat(),
            self.ticker,
            segment,
            self.open,
            self.high,
            self.low,
            self.close,
            self.tr if self.tr is not None else "",
            self.atr if self.atr is not None else "",
            "",  # volume (optional)
            "TRUE" if self.gap_filled else "FALSE",
            get_current_ist().isoformat(),  # created_at
        ]


class ATREngine:
    """
    14-period ATR calculator with per-ticker state management.

    Lifecycle:
    1. Cold start: accumulate 14 TR values, then compute mean
    2. Steady state: use Wilder's smoothing formula
    3. State persisted via checkpoint_manager for recovery
    """

    def __init__(self):
        self._state: Dict[str, ATRState] = {}

    def get_state(self) -> Dict[str, dict]:
        """
        Export full ATR state for checkpointing.

        Returns serializable dict suitable for JSON storage.
        """
        result = {}
        for ticker, state in self._state.items():
            result[ticker] = {
                "prev_close": state.prev_close,
                "prev_atr": state.prev_atr,
                "tr_history": state.tr_history,
                "candle_count": state.candle_count,
            }
        return result

    def load_state(self, state_dict: Dict[str, dict]) -> None:
        """
        Load ATR state from checkpoint.

        Args:
            state_dict: Dict from get_state() or checkpoint file
        """
        self._state.clear()
        for ticker, data in state_dict.items():
            self._state[ticker] = ATRState(
                prev_close=data.get("prev_close"),
                prev_atr=data.get("prev_atr"),
                tr_history=data.get("tr_history", []),
                candle_count=data.get("candle_count", 0),
            )
        logger.info(f"ATR_STATE_LOADED | tickers={len(self._state)}")

    @staticmethod
    def compute_tr(high: float, low: float, prev_close: Optional[float]) -> float:
        """
        Compute True Range using the 3-way max formula.

        TR = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )

        If prev_close is None (first candle), TR = high - low.
        """
        range_hl = high - low

        if prev_close is None:
            return round(range_hl, ATR_PRECISION)

        tr = max(
            range_hl,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        return round(tr, ATR_PRECISION)

    def compute_atr(self, ticker: str, tr: float) -> Optional[float]:
        """
        Compute ATR for a ticker given the current TR.

        - Cold start (< 14 candles): accumulate TR, return None
        - Warmup complete (exactly 14): ATR = mean of 14 TRs
        - Steady state: ATR = ((prev_atr * 13) + TR) / 14

        Returns None during warmup, else rounded ATR value.
        """
        state = self._state.get(ticker)
        if state is None:
            state = ATRState()
            self._state[ticker] = state

        state.candle_count += 1

        # Cold start — accumulating TR history
        if state.prev_atr is None:
            state.tr_history.append(tr)

            if len(state.tr_history) < ATR_PERIOD:
                # Still warming up
                return None

            if len(state.tr_history) == ATR_PERIOD:
                # Warmup complete — compute initial ATR as mean
                atr = sum(state.tr_history) / ATR_PERIOD
                atr = round(atr, ATR_PRECISION)
                state.prev_atr = atr
                state.tr_history.clear()  # No longer needed

                # Validation
                if atr < 0:
                    logger.error(f"ATR_NEGATIVE | ticker={ticker} | atr={atr}")
                    atr = 0.0
                    state.prev_atr = 0.0

                return atr

        # Steady state — Wilder's smoothing
        atr = ((state.prev_atr * (ATR_PERIOD - 1)) + tr) / ATR_PERIOD
        atr = round(atr, ATR_PRECISION)

        # Validation: non-negative
        if atr < 0:
            logger.error(f"ATR_NEGATIVE | ticker={ticker} | atr={atr}")
            atr = 0.0

        # Validation: 3x jump warning
        if state.prev_atr > 0 and atr > 3 * state.prev_atr:
            logger.warning(
                f"ATR_JUMP | ticker={ticker} | "
                f"prev_atr={state.prev_atr} | new_atr={atr} | "
                f"tr={tr} | ratio={atr/state.prev_atr:.2f}x"
            )

        state.prev_atr = atr
        return atr

    def process_batch(
        self, candles: Dict[str, OHLCCandle]
    ) -> List[EnrichedCandle]:
        """
        Process a batch of 178 candles: compute TR + ATR for each.

        Args:
            candles: Dict[ticker, OHLCCandle] from candle_aggregator

        Returns:
            List of EnrichedCandle with TR and ATR fields populated.
        """
        enriched = []

        for ticker, candle in candles.items():
            # Get previous close for TR computation
            state = self._state.get(ticker)
            prev_close = state.prev_close if state else None

            # Compute TR
            tr = self.compute_tr(candle.high, candle.low, prev_close)

            # Compute ATR
            atr = self.compute_atr(ticker, tr)

            # Update prev_close for next window
            if ticker not in self._state:
                self._state[ticker] = ATRState()
            self._state[ticker].prev_close = candle.close

            enriched.append(EnrichedCandle(
                ticker=ticker,
                window_start=candle.window_start,
                open=candle.open,
                high=candle.high,
                low=candle.low,
                close=candle.close,
                tr=tr,
                atr=atr,
                tick_count=candle.tick_count,
                gap_filled=candle.gap_filled,
            ))

        logger.info(
            f"ATR_BATCH | tickers={len(enriched)} | "
            f"with_atr={sum(1 for e in enriched if e.atr is not None)}"
        )

        return enriched

    def get_atr_summary(self) -> Dict[str, dict]:
        """
        Export current ATR state summary for atr_state sheet sync.

        Returns dict suitable for writing to Google Sheets atr_state sheet.
        """
        summary = {}
        for ticker, state in self._state.items():
            summary[ticker] = {
                "last_close": state.prev_close,
                "last_atr": state.prev_atr,
                "candle_count": state.candle_count,
            }
        return summary
