"""
WebSocket Client — Kotak Neo Tick Ingestion

Maintains persistent WebSocket connection, subscribes to 178 instruments
in batches, and feeds ticks into the TickBuffer using exchange timestamps.

🔒1: Uses exchange timestamp (not system clock) for window assignment.
🔒4: Trading-calendar-aware lifecycle (skips holidays, supports special sessions).
🔒7: Callback latency instrumentation with periodic percentile reporting.
"""

import collections
import statistics
import threading
import time as time_module
from datetime import datetime
from typing import Callable, Optional

from config.instruments import INSTRUMENTS, INSTRUMENT_BY_TOKEN
from config.settings import (
    CALLBACK_LATENCY_MAX_US,
    CALLBACK_LATENCY_WARN_US,
    HEARTBEAT_SILENCE_TIMEOUT_S,
    IST,
    LATENCY_SAMPLE_SIZE,
    WS_SUBSCRIBE_BATCH_SIZE,
)
from modules.aggregator.tick_buffer import TickBuffer
from utils.logger import get_logger
from utils.time_utils import assign_tick_to_window

logger = get_logger("websocket.ws_client")


class WSClient:
    """
    Kotak Neo WebSocket client for tick-level market data ingestion.

    Responsibilities:
    - Persistent connection lifecycle (connect/disconnect)
    - Batched instrument subscription
    - Non-blocking tick parsing and buffer push
    - Heartbeat monitoring and reconnect triggering
    - Callback latency instrumentation
    """

    def __init__(
        self,
        tick_buffer: TickBuffer,
        authenticator=None,
        on_reconnect_needed: Optional[Callable] = None,
    ):
        self._tick_buffer = tick_buffer
        self._authenticator = authenticator
        self._on_reconnect_needed = on_reconnect_needed

        # Connection state
        self._client = None
        self._connected = False
        self._subscribed = False

        # Heartbeat tracking
        self._last_tick_monotonic: float = 0.0
        self._last_tick_time: Optional[datetime] = None

        # 🔒7 Latency instrumentation
        self._latency_samples = collections.deque(maxlen=LATENCY_SAMPLE_SIZE)
        self._total_ticks_received: int = 0
        self._tick_parse_errors: int = 0

        # Thread safety
        self._lock = threading.Lock()

    def connect(self) -> None:
        """
        Establish WebSocket connection using authenticated client.
        """
        if self._authenticator is None:
            raise RuntimeError("Authenticator not set — cannot connect")

        try:
            self._client = self._authenticator.get_client()
            self._connected = True
            logger.info("WEBSOCKET_CONNECTED")
        except Exception as e:
            logger.error(f"WEBSOCKET_CONNECT_FAILED | error={e}")
            raise

    def subscribe(self) -> None:
        """
        Subscribe to all instruments in batches to avoid throttling.
        """
        if not self._connected or self._client is None:
            raise RuntimeError("WebSocket not connected")

        total = len(INSTRUMENTS)
        batch_size = WS_SUBSCRIBE_BATCH_SIZE

        for i in range(0, total, batch_size):
            batch = INSTRUMENTS[i : i + batch_size]
            instrument_list = [
                {"instrument_token": inst.token, "exchange_segment": inst.segment}
                for inst in batch
            ]

            try:
                self._client.subscribe(
                    instrument_tokens=instrument_list,
                    isIndex=False,
                )
                logger.info(
                    f"SUBSCRIBED_BATCH | start={i} | "
                    f"count={len(batch)} | total={total}"
                )
                # Small delay between batches to avoid throttling
                time_module.sleep(0.5)
            except Exception as e:
                logger.error(
                    f"SUBSCRIBE_FAILED | batch_start={i} | error={e}"
                )
                raise

        self._subscribed = True

        # Register callbacks
        self._client.on_message = self._on_message
        self._client.on_error = self._on_error
        self._client.on_close = self._on_close

        logger.info(f"SUBSCRIPTION_COMPLETE | instruments={total}")

    def _on_message(self, message: dict) -> None:
        """
        🔒1 Non-blocking tick handler — HOT PATH.

        Extracts exchange timestamp, assigns window, pushes to buffer.
        No I/O, no logging, no computation beyond minimum necessary.

        🔒7 Records callback execution latency for monitoring.
        """
        t0 = time_module.perf_counter_ns()

        try:
            # Extract fields from tick message
            # Kotak Neo message format varies — adapt field names as needed
            token = str(message.get("tk", message.get("instrument_token", "")))
            ltp = message.get("ltp", message.get("last_traded_price"))

            if ltp is None or token == "":
                return

            ltp = float(ltp)

            # 🔒1 Extract EXCHANGE timestamp (not system time)
            exchange_ts_raw = message.get(
                "exchange_timestamp",
                message.get("ft", message.get("feed_time")),
            )

            if exchange_ts_raw is not None:
                # Parse exchange timestamp
                if isinstance(exchange_ts_raw, (int, float)):
                    exchange_ts = datetime.fromtimestamp(
                        exchange_ts_raw, tz=IST
                    )
                elif isinstance(exchange_ts_raw, str):
                    exchange_ts = datetime.fromisoformat(exchange_ts_raw)
                    if exchange_ts.tzinfo is None:
                        exchange_ts = exchange_ts.replace(tzinfo=IST)
                else:
                    exchange_ts = datetime.now(tz=IST)
            else:
                # Fallback: last resort if exchange timestamp missing
                exchange_ts = datetime.now(tz=IST)

            # Resolve token to ticker symbol
            instrument = INSTRUMENT_BY_TOKEN.get(token)
            if instrument is None:
                return

            # 🔒1 Assign tick to window using exchange timestamp
            try:
                window_start = assign_tick_to_window(exchange_ts)
            except ValueError:
                # Tick outside market hours — silently drop
                return

            # Push to buffer (thread-safe, O(1))
            self._tick_buffer.update(instrument.symbol, ltp, window_start)

            # Update heartbeat
            self._last_tick_monotonic = time_module.monotonic()
            self._total_ticks_received += 1

        except Exception:
            self._tick_parse_errors += 1

        finally:
            # 🔒7 Record latency (always, even on error)
            elapsed_ns = time_module.perf_counter_ns() - t0
            self._latency_samples.append(elapsed_ns)

    def _on_error(self, error) -> None:
        """Handle WebSocket errors."""
        logger.error(f"WEBSOCKET_ERROR | error={error}")

    def _on_close(self, *args) -> None:
        """Handle WebSocket disconnection."""
        logger.warning("WEBSOCKET_CLOSED")
        self._connected = False
        self._subscribed = False

        if self._on_reconnect_needed:
            self._on_reconnect_needed()

    def disconnect(self) -> None:
        """Gracefully disconnect WebSocket."""
        try:
            if self._client and self._connected:
                self._client.close()
        except Exception as e:
            logger.warning(f"DISCONNECT_ERROR | error={e}")
        finally:
            self._connected = False
            self._subscribed = False
            logger.info("WEBSOCKET_DISCONNECTED")

    def check_heartbeat(self) -> bool:
        """
        Check if ticks are flowing.

        Returns False if no tick received for HEARTBEAT_SILENCE_TIMEOUT_S.
        """
        if self._last_tick_monotonic == 0:
            return True  # Haven't received first tick yet

        elapsed = time_module.monotonic() - self._last_tick_monotonic
        if elapsed > HEARTBEAT_SILENCE_TIMEOUT_S:
            logger.warning(
                f"HEARTBEAT_TIMEOUT | silence_seconds={elapsed:.1f} | "
                f"threshold={HEARTBEAT_SILENCE_TIMEOUT_S}"
            )
            return False
        return True

    def get_latency_report(self) -> dict:
        """
        🔒7 Compute and return callback latency percentiles.

        Returns dict with p50, p95, p99, max in microseconds.
        Resets the sample buffer after reporting.
        """
        samples = list(self._latency_samples)
        self._latency_samples.clear()

        if not samples:
            return {
                "p50_us": 0, "p95_us": 0, "p99_us": 0, "max_us": 0,
                "sample_count": 0, "total_ticks": self._total_ticks_received,
                "parse_errors": self._tick_parse_errors,
            }

        # Convert ns to μs
        samples_us = [s / 1000 for s in samples]
        samples_us.sort()

        n = len(samples_us)
        p50 = samples_us[int(n * 0.50)]
        p95 = samples_us[int(n * 0.95)]
        p99 = samples_us[int(n * 0.99)]
        max_us = samples_us[-1]

        report = {
            "p50_us": round(p50, 1),
            "p95_us": round(p95, 1),
            "p99_us": round(p99, 1),
            "max_us": round(max_us, 1),
            "sample_count": n,
            "total_ticks": self._total_ticks_received,
            "parse_errors": self._tick_parse_errors,
        }

        # 🔒7 Alert if p99 exceeds threshold
        if p99 > CALLBACK_LATENCY_WARN_US:
            logger.warning(
                f"CALLBACK_LATENCY_HIGH | p99={p99:.1f}μs | "
                f"threshold={CALLBACK_LATENCY_WARN_US}μs"
            )

        if max_us > CALLBACK_LATENCY_MAX_US:
            logger.warning(
                f"CALLBACK_LATENCY_MAX_EXCEEDED | max={max_us:.1f}μs | "
                f"threshold={CALLBACK_LATENCY_MAX_US}μs"
            )

        return report

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def is_subscribed(self) -> bool:
        return self._subscribed
