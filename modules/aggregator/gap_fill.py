"""
Gap-Fill Logic for Illiquid Instruments

Synthesizes flat candles for instruments that had zero ticks in a given window,
but have traded previously in the session. This prevents state drift in ATR
and ensures contiguous data.

🔒8: Gap-Fill Logic Implementation
"""

from datetime import datetime
from typing import Dict, List, Tuple

from modules.aggregator.tick_buffer import OHLCCandle
from utils.logger import get_logger

logger = get_logger("aggregator.gap_fill")


class GapFiller:
    """
    Synthesizes flat candles for symbols with missing ticks.

    Stateful class that holds the last known close price for each symbol
    across the entire trading session.
    Not thread-safe; must only be called from the main scheduler thread.
    """

    def __init__(self):
        self._last_close: Dict[str, float] = {}

    def fill(
        self,
        candles: Dict[str, OHLCCandle],
        expected_symbols: List[str],
        window_start: datetime,
    ) -> Tuple[Dict[str, OHLCCandle], List[str]]:
        """
        Fill missing candles with flat candles based on previous close.

        Args:
            candles: Extracted candles for the current window.
                     Will be mutated.
            expected_symbols: Complete list of all instruments.
            window_start: The start time of the current window.

        Returns:
            Tuple of (merged_candles, unfillable_symbols)
        """
        filled_count = 0
        unfillable = []
        filled_symbols = []

        for symbol in expected_symbols:
            if symbol not in candles:
                # Symbol is missing from the current window snapshot
                last_close = self._last_close.get(symbol)
                
                if last_close is not None:
                    # Synthesize flat candle
                    flat_candle = OHLCCandle(
                        window_start=window_start,
                        open=last_close,
                        high=last_close,
                        low=last_close,
                        close=last_close,
                        tick_count=0,
                        gap_filled=True,
                    )
                    candles[symbol] = flat_candle
                    filled_count += 1
                    filled_symbols.append(symbol)
                else:
                    # Cold start: symbol hasn't traded yet at all today
                    unfillable.append(symbol)

        # Update last_close for all symbols in the (now optionally gap-filled) dict
        for symbol, candle in candles.items():
            self._last_close[symbol] = candle.close

        if filled_count > 0:
            logger.info(
                f"GAP_FILL | window={window_start.isoformat()} | "
                f"filled={filled_count} | unfillable={len(unfillable)} | "
                f"symbols={filled_symbols}"
            )

        return candles, unfillable
