"""
Kotak Neo 5-Minute Volatility Harvester — Main Orchestrator

Entry point that wires all modules together and runs the 3-thread architecture:
- Thread 1: Scheduler (candle finalization timer)
- Thread 2: WebSocket listener (tick ingestion)
- Thread 3: Sheets writer (write pipeline consumer)

Lifecycle:
1. Calendar check → skip if holiday
2. Authenticate → WebSocket connect → subscribe
3. Initialize aggregator for today's session
4. Startup reconciliation (checkpoint vs Sheets)
5. Schedule finalization at each 5-minute boundary
6. Run until session close
7. End-of-day cleanup + final checkpoint
"""

import signal
import sys
import threading
import time as time_module
from datetime import date, datetime, timedelta

from config.settings import (
    CANDLE_INTERVAL_MINUTES,
    IST,
    WINDOW_FREEZE_MS,
)
from config.trading_calendar import trading_calendar
from modules.aggregator.candle_aggregator import CandleAggregator
from modules.aggregator.tick_buffer import TickBuffer
from modules.atr.atr_engine import ATREngine
from modules.auth.authenticator import Authenticator, AuthenticationFailed
from modules.pipeline.write_pipeline import WritePipeline
from modules.recovery.checkpoint_manager import CheckpointManager
from modules.sheets.schema_manager import SchemaManager
from modules.sheets.sheets_client import SheetsClient
from modules.websocket.ws_client import WSClient
from utils.logger import get_logger
from utils.time_utils import (
    generate_finalization_times,
    get_current_ist,
    is_market_hours,
)

logger = get_logger("main")


class VolatilityHarvester:
    """
    Main orchestrator for the 5-minute Volatility Harvester pipeline.

    Coordinates all modules across 3 threads for the full trading session.
    """

    def __init__(self):
        # Core modules
        self._authenticator = Authenticator()
        self._tick_buffer = TickBuffer()
        self._aggregator = CandleAggregator(self._tick_buffer)
        self._atr_engine = ATREngine()
        self._sheets_client = SheetsClient()
        self._schema_manager = SchemaManager(self._sheets_client)
        self._write_pipeline = WritePipeline(self._sheets_client, self._schema_manager)
        self._checkpoint_mgr = CheckpointManager()
        self._ws_client = WSClient(
            self._tick_buffer,
            self._authenticator,
            on_reconnect_needed=self._handle_reconnect,
        )

        # State
        self._running = False
        self._today: date = None
        self._session_open = None
        self._session_close = None

        # Shutdown handling
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def run(self) -> None:
        """Main entry point — execute the full trading session."""
        logger.info("=" * 60)
        logger.info("VOLATILITY HARVESTER STARTING")
        logger.info("=" * 60)

        try:
            self._today = get_current_ist().date()

            # 🔒4: Calendar check
            if not trading_calendar.is_trading_day(self._today):
                holiday_name = trading_calendar.get_holiday_name(self._today)
                logger.info(
                    f"NON_TRADING_DAY | date={self._today} | "
                    f"reason={holiday_name or 'weekend'}"
                )
                next_day = trading_calendar.get_next_trading_day(self._today)
                logger.info(f"NEXT_TRADING_DAY | date={next_day}")
                return

            # Get session hours (may be special session)
            self._session_open, self._session_close = (
                trading_calendar.get_session_hours(self._today)
            )
            logger.info(
                f"SESSION_HOURS | open={self._session_open} | "
                f"close={self._session_close}"
            )

            # Phase 1: Setup
            self._setup()

            # Phase 2: Run session
            self._running = True
            self._run_session()

        except AuthenticationFailed as e:
            logger.error(f"FATAL_AUTH_ERROR | {e}")
            sys.exit(1)
        except KeyboardInterrupt:
            logger.info("KEYBOARD_INTERRUPT")
        except Exception as e:
            logger.error(f"FATAL_ERROR | {e}", exc_info=True)
        finally:
            self._cleanup()

    def _setup(self) -> None:
        """Initialize all modules and perform startup reconciliation."""

        # 1. Authenticate
        logger.info("PHASE: Authentication")
        self._authenticator.login()

        # 2. Initialize Sheets schema
        logger.info("PHASE: Sheets Schema")
        self._schema_manager.initialize_if_empty()

        # 3. 🔒5: Startup reconciliation
        logger.info("PHASE: Startup Reconciliation")
        reconciled_state, source = self._checkpoint_mgr.reconcile_state_on_startup(
            self._sheets_client
        )

        if reconciled_state:
            self._atr_engine.load_state(reconciled_state)
            logger.info(f"ATR_STATE_RESTORED | source={source}")

        # Log startup event
        self._schema_manager.log_event(
            "INFO", "SESSION_START",
            details=f"date={self._today} | state_source={source}"
        )

        # 4. Initialize aggregator
        self._aggregator.initialize_for_session(
            target_date=self._today,
            session_open=self._session_open,
            session_close=self._session_close,
        )

        # 5. Start write pipeline consumer (Thread 3)
        self._write_pipeline.start_consumer()

        # 6. Connect WebSocket (Thread 2)
        self._ws_client.connect()
        self._ws_client.subscribe()

    def _run_session(self) -> None:
        """
        Main loop: wait for each 5-minute boundary and finalize candles.

        Thread 1 (Scheduler) — runs in main thread.
        """
        finalization_times = generate_finalization_times(
            target_date=self._today,
            session_open=self._session_open,
            session_close=self._session_close,
        )

        logger.info(f"FINALIZATION_SCHEDULE | boundaries={len(finalization_times)}")

        # Find the first boundary we haven't passed yet
        now = get_current_ist()
        start_idx = 0
        for i, ft in enumerate(finalization_times):
            if ft > now:
                start_idx = i
                break

        if start_idx > 0:
            logger.info(f"SKIPPING_PAST_BOUNDARIES | count={start_idx}")

        # Start collecting for the first active window
        windows = self._aggregator.get_window_boundaries()
        if start_idx < len(windows):
            self._aggregator.start_window(windows[start_idx])

        # Latency reporting interval
        latency_report_interval = 60  # seconds
        last_latency_report = time_module.monotonic()

        for i in range(start_idx, len(finalization_times)):
            if not self._running:
                break

            boundary = finalization_times[i]
            next_window = windows[i + 1] if i + 1 < len(windows) else None

            # Wait until boundary
            while self._running:
                now = get_current_ist()
                remaining = (boundary - now).total_seconds()

                if remaining <= 0:
                    break

                # Check heartbeat periodically
                if not self._ws_client.check_heartbeat():
                    self._handle_reconnect()

                # 🔒7: Periodic latency report
                if time_module.monotonic() - last_latency_report > latency_report_interval:
                    report = self._ws_client.get_latency_report()
                    if report["sample_count"] > 0:
                        logger.info(
                            f"LATENCY_REPORT | "
                            f"p50={report['p50_us']}μs | "
                            f"p95={report['p95_us']}μs | "
                            f"p99={report['p99_us']}μs | "
                            f"max={report['max_us']}μs | "
                            f"samples={report['sample_count']} | "
                            f"total_ticks={report['total_ticks']}"
                        )
                    last_latency_report = time_module.monotonic()

                time_module.sleep(min(remaining, 1.0))

            if not self._running:
                break

            # 🔒2: Freeze + finalize sequence
            self._finalize_at_boundary(i, next_window)

        logger.info("SESSION_COMPLETE")

    def _finalize_at_boundary(
        self, boundary_index: int, next_window_start
    ) -> None:
        """
        Execute the freeze → snapshot → ATR → enqueue → checkpoint cycle.

        🔒2: freeze → wait WINDOW_FREEZE_MS → snapshot
        """
        # Step 1: Freeze
        self._aggregator.begin_freeze()

        # Step 2: Wait for in-flight ticks
        time_module.sleep(WINDOW_FREEZE_MS / 1000.0)

        # Step 3: Finalize (snapshot + validate)
        window_start, candles = self._aggregator.finalize_window()

        if window_start is None or not candles:
            logger.warning(f"EMPTY_FINALIZATION | index={boundary_index}")
            if next_window_start:
                self._aggregator.transition_to_next_window(next_window_start)
            return

        # Step 4: ATR computation
        enriched = self._atr_engine.process_batch(candles)

        # Step 5: Enqueue for Sheets write (Thread 3)
        self._write_pipeline.enqueue(window_start, enriched)

        # Step 6: Checkpoint (before Sheets confirmation)
        atr_summary = self._atr_engine.get_atr_summary()
        self._checkpoint_mgr.save_checkpoint(
            atr_state=self._atr_engine.get_state(),
            last_window=window_start,
            sheets_write_confirmed=False,  # Will be updated after write
        )

        # Step 7: Sync ATR state to Sheets
        self._write_pipeline.sync_atr_state(atr_summary)

        # Step 8: Transition to next window
        if next_window_start:
            self._aggregator.transition_to_next_window(next_window_start)

    def _handle_reconnect(self) -> None:
        """Handle WebSocket disconnection — reconnect cycle."""
        logger.warning("RECONNECT_TRIGGERED")

        try:
            self._ws_client.disconnect()
            time_module.sleep(2)
            self._authenticator.refresh_session()
            self._ws_client.connect()
            self._ws_client.subscribe()
            logger.info("RECONNECT_SUCCESS")

            self._schema_manager.log_event(
                "WARNING", "RECONNECT",
                details="WebSocket reconnected after silence"
            )
        except Exception as e:
            logger.error(f"RECONNECT_FAILED | error={e}")

    def _handle_shutdown(self, signum, frame) -> None:
        """Graceful shutdown on SIGINT/SIGTERM."""
        logger.info(f"SHUTDOWN_SIGNAL | signal={signum}")
        self._running = False

    def _cleanup(self) -> None:
        """End-of-day cleanup."""
        logger.info("CLEANUP_START")

        # Disconnect WebSocket
        try:
            self._ws_client.disconnect()
        except Exception:
            pass

        # Stop write pipeline
        try:
            self._write_pipeline.stop_consumer()
        except Exception:
            pass

        # Final checkpoint
        try:
            now = get_current_ist()
            self._checkpoint_mgr.save_checkpoint(
                atr_state=self._atr_engine.get_state(),
                last_window=now,
                sheets_write_confirmed=True,
            )
        except Exception:
            pass

        # Log session end
        try:
            self._schema_manager.log_event(
                "INFO", "SESSION_END",
                details=f"date={self._today}"
            )

            # Final latency report
            report = self._ws_client.get_latency_report()
            logger.info(
                f"FINAL_LATENCY_REPORT | "
                f"total_ticks={report['total_ticks']} | "
                f"parse_errors={report['parse_errors']}"
            )
        except Exception:
            pass

        logger.info("CLEANUP_COMPLETE")
        logger.info("=" * 60)


def main():
    harvester = VolatilityHarvester()
    harvester.run()


if __name__ == "__main__":
    main()
