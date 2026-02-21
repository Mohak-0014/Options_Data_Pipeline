"""
Write Pipeline — Queue-Based Producer-Consumer with Deduplication and Fallback

Manages the write path from ATR Engine output to Google Sheets.
Runs as a consumer on Thread 3.

🔒3: Restart-safe duplicate prevention using deterministic ID reconciliation.
🔒6: Validates append response row counts to detect silent partial writes.
"""

import json
import queue
import threading
import time as time_module
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

from config.instruments import INSTRUMENT_BY_SYMBOL
from config.settings import (
    FALLBACK_DIR,
    IST,
    MAX_RETRIES,
    RETRY_BASE_DELAY_S,
    TICKER_COUNT,
)
from modules.atr.atr_engine import EnrichedCandle
from modules.sheets.sheets_client import SheetsClient
from modules.sheets.schema_manager import SchemaManager
from utils.id_generator import generate_row_id
from utils.logger import get_logger

logger = get_logger("pipeline.write_pipeline")


class WritePipeline:
    """
    Queue-based write pipeline for Google Sheets persistence.

    Producer side:
    - Accepts batches of EnrichedCandle from ATR Engine
    - Formats rows and generates deterministic IDs
    - Enqueues onto thread-safe queue

    Consumer side (Thread 3):
    - Dequeues batches
    - Performs ID-based deduplication against Sheets
    - Appends with retry and response validation
    - Falls back to local JSON on exhausted retries
    """

    def __init__(self, sheets_client: SheetsClient, schema_manager: SchemaManager):
        self._sheets_client = sheets_client
        self._schema_manager = schema_manager
        self._queue: queue.Queue = queue.Queue()
        self._consumer_thread: Optional[threading.Thread] = None
        self._running = False
        self._fallback_dir = FALLBACK_DIR

        # Ensure fallback directory exists
        self._fallback_dir.mkdir(parents=True, exist_ok=True)

    def start_consumer(self) -> None:
        """Start the consumer thread (Thread 3)."""
        self._running = True
        self._consumer_thread = threading.Thread(
            target=self._consumer_loop,
            name="SheetsWriter",
            daemon=True,
        )
        self._consumer_thread.start()
        logger.info("WRITE_CONSUMER_STARTED")

    def stop_consumer(self) -> None:
        """Stop the consumer thread gracefully."""
        self._running = False
        # Send sentinel to unblock queue.get()
        self._queue.put(None)
        if self._consumer_thread:
            self._consumer_thread.join(timeout=30)
        logger.info("WRITE_CONSUMER_STOPPED")

    def enqueue(
        self,
        window_start: datetime,
        enriched_candles: List[EnrichedCandle],
    ) -> None:
        """
        Producer: format batch and enqueue for writing.

        Generates deterministic IDs for each row.
        """
        rows = []
        row_ids = set()

        for candle in enriched_candles:
            row_id = generate_row_id(candle.ticker, candle.window_start)
            row_ids.add(row_id)

            segment = ""
            inst = INSTRUMENT_BY_SYMBOL.get(candle.ticker)
            if inst:
                segment = inst.segment

            row = candle.to_row(row_id, segment)
            rows.append(row)

        batch = {
            "window_start": window_start.isoformat(),
            "rows": rows,
            "row_ids": sorted(row_ids),
            "expected_count": len(rows),
        }

        self._queue.put(batch)
        logger.info(
            f"BATCH_ENQUEUED | window={window_start.time()} | rows={len(rows)}"
        )

    def _consumer_loop(self) -> None:
        """Consumer thread main loop."""
        while self._running:
            try:
                batch = self._queue.get(timeout=5)
                if batch is None:
                    break  # Sentinel — shutdown

                # First, try to flush any pending fallback data
                self._flush_fallback()

                # Process current batch
                self._process_batch(batch)

            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"CONSUMER_ERROR | error={e}", exc_info=True)

    def _process_batch(self, batch: dict) -> None:
        """
        Process a single write batch with deduplication and retry.

        🔒3: Computes delta between batch IDs and existing IDs in Sheets.
        🔒6: Validates append response row count.
        """
        window_str = batch["window_start"]
        all_rows = batch["rows"]
        all_ids = set(batch["row_ids"])
        expected = batch["expected_count"]

        # 🔒3: ID-based reconciliation
        try:
            window_dt = datetime.fromisoformat(window_str)
            existing_ids = self._sheets_client.get_existing_ids_for_window(window_dt)
        except Exception as e:
            logger.warning(f"DEDUP_CHECK_FAILED | error={e} | proceeding with full batch")
            existing_ids = set()

        # Compute delta
        ids_to_write = all_ids - existing_ids

        if not ids_to_write:
            logger.info(
                f"DEDUP_SKIP | window={window_str} | "
                f"all {expected} rows already exist"
            )
            return

        # Filter rows to only those needing write
        if len(ids_to_write) < len(all_rows):
            rows_to_write = [
                row for row in all_rows if row[0] in ids_to_write
            ]
            logger.info(
                f"DEDUP_PARTIAL | window={window_str} | "
                f"existing={len(existing_ids)} | to_write={len(rows_to_write)} | "
                f"total={expected}"
            )
        else:
            rows_to_write = all_rows
            logger.info(
                f"DEDUP_FRESH | window={window_str} | rows={len(rows_to_write)}"
            )

        # Write with retry
        success = self._write_with_retry(rows_to_write, window_str)

        if success:
            # Log event
            self._schema_manager.log_event(
                "INFO", "CANDLE_WRITTEN",
                window=window_str,
                details=f"rows={len(rows_to_write)}"
            )
        else:
            # Save to fallback
            self._save_to_fallback(batch)

    def _write_with_retry(self, rows: List[list], window_str: str) -> bool:
        """
        Write rows to Sheets with exponential backoff retry.

        🔒6: Validates `updatedRows` in API response.

        Returns True on success, False on all retries exhausted.
        """
        worksheet = self._sheets_client.get_sheet("market_data")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                delay = RETRY_BASE_DELAY_S * (2 ** (attempt - 1))

                response = worksheet.append_rows(
                    rows, value_input_option="RAW"
                )

                # 🔒6: Validate response row count
                updated_rows = (
                    response.get("updates", {}).get("updatedRows", 0)
                    if isinstance(response, dict)
                    else len(rows)  # gspread may not return structured response
                )

                if isinstance(response, dict) and updated_rows != len(rows):
                    logger.error(
                        f"PARTIAL_WRITE | window={window_str} | "
                        f"expected={len(rows)} | actual={updated_rows} | "
                        f"attempt={attempt}"
                    )
                    if attempt < MAX_RETRIES:
                        time_module.sleep(delay)
                        continue
                    return False

                logger.info(
                    f"WRITE_SUCCESS | window={window_str} | "
                    f"rows={len(rows)} | attempt={attempt}"
                )
                return True

            except Exception as e:
                logger.warning(
                    f"WRITE_FAILED | window={window_str} | "
                    f"attempt={attempt}/{MAX_RETRIES} | error={e}"
                )
                if attempt < MAX_RETRIES:
                    delay = RETRY_BASE_DELAY_S * (2 ** (attempt - 1))
                    logger.info(f"RETRY_BACKOFF | delay={delay}s")
                    time_module.sleep(delay)

        logger.error(
            f"WRITE_EXHAUSTED | window={window_str} | "
            f"max_retries={MAX_RETRIES}"
        )
        return False

    def _save_to_fallback(self, batch: dict) -> None:
        """
        Save failed batch to local JSON for later retry.

        File: data/fallback/unsent_backup.json
        Appends to existing fallback data if present.
        """
        fallback_file = self._fallback_dir / "unsent_backup.json"

        try:
            # Load existing fallback data
            existing = []
            if fallback_file.exists():
                with open(fallback_file, "r", encoding="utf-8") as f:
                    existing = json.load(f)

            existing.append(batch)

            with open(fallback_file, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2)

            logger.warning(
                f"FALLBACK_SAVED | window={batch['window_start']} | "
                f"rows={batch['expected_count']} | "
                f"total_pending={len(existing)}"
            )

        except Exception as e:
            logger.error(f"FALLBACK_SAVE_FAILED | error={e}")

    def _flush_fallback(self) -> None:
        """
        Attempt to write any pending fallback data.

        Called before processing new batches.
        Uses 🔒3 ID-based reconciliation to avoid duplicates.
        """
        fallback_file = self._fallback_dir / "unsent_backup.json"

        if not fallback_file.exists():
            return

        try:
            with open(fallback_file, "r", encoding="utf-8") as f:
                pending = json.load(f)

            if not pending:
                fallback_file.unlink(missing_ok=True)
                return

            logger.info(f"FALLBACK_FLUSH_START | batches={len(pending)}")

            remaining = []
            for batch in pending:
                try:
                    self._process_batch(batch)
                except Exception as e:
                    logger.error(f"FALLBACK_FLUSH_ERROR | error={e}")
                    remaining.append(batch)

            if remaining:
                with open(fallback_file, "w", encoding="utf-8") as f:
                    json.dump(remaining, f, indent=2)
                logger.warning(f"FALLBACK_PARTIAL_FLUSH | remaining={len(remaining)}")
            else:
                fallback_file.unlink(missing_ok=True)
                logger.info("FALLBACK_FLUSH_COMPLETE | all_batches_flushed")

        except Exception as e:
            logger.error(f"FALLBACK_FLUSH_FAILED | error={e}")

    def sync_atr_state(self, atr_summary: Dict[str, dict]) -> None:
        """
        Overwrite the atr_state sheet with current ATR values.

        Called after each successful market_data write.
        """
        from utils.time_utils import get_current_ist

        worksheet = self._sheets_client.get_sheet("atr_state")
        now_str = get_current_ist().isoformat()

        try:
            # Clear existing data (keep header)
            worksheet.resize(rows=1)

            # Build rows
            rows = []
            for ticker, state in sorted(atr_summary.items()):
                rows.append([
                    ticker,
                    state.get("last_close", ""),
                    state.get("last_atr", ""),
                    state.get("last_timestamp", ""),
                    now_str,
                ])

            if rows:
                worksheet.append_rows(rows, value_input_option="RAW")
                # Resize to fit
                worksheet.resize(rows=len(rows) + 1)

            logger.info(f"ATR_STATE_SYNCED | tickers={len(rows)}")

        except Exception as e:
            logger.error(f"ATR_STATE_SYNC_FAILED | error={e}")

    def get_queue_size(self) -> int:
        """Return current write queue size."""
        return self._queue.qsize()
