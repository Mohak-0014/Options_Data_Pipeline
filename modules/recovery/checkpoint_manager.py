"""
Checkpoint Manager — Atomic State Persistence and Recovery

Saves ATR engine state and window progress locally every 5 minutes.
Enables crash recovery without recalculating history.

🔒3: Includes sheets_write_confirmed flag for partial-write awareness.
🔒5: reconcile_state_on_startup() cross-validates local checkpoint
     against Google Sheets state.
"""

import json
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

from config.settings import CHECKPOINT_DIR, IST, MAX_CHECKPOINT_FILES
from utils.logger import get_logger

logger = get_logger("recovery.checkpoint_manager")


class CheckpointManager:
    """
    Manages ATR state checkpointing and startup recovery.

    Features:
    - Atomic save (write to temp → rename) to prevent corruption
    - Rotating checkpoint files (keep last N)
    - Corrupt checkpoint detection with fallback
    - Startup reconciliation with Google Sheets
    """

    CHECKPOINT_FILENAME = "checkpoint.json"

    def __init__(self, checkpoint_dir: Optional[Path] = None):
        self._dir = checkpoint_dir or CHECKPOINT_DIR
        self._dir.mkdir(parents=True, exist_ok=True)

    @property
    def checkpoint_path(self) -> Path:
        return self._dir / self.CHECKPOINT_FILENAME

    def save_checkpoint(
        self,
        atr_state: Dict[str, dict],
        last_window: datetime,
        sheets_write_confirmed: bool = True,
    ) -> None:
        """
        Atomically save checkpoint to disk.

        Uses temp file + rename for crash safety.
        Rotates old checkpoints (keeps last N).

        🔒3: sheets_write_confirmed flag indicates whether the Sheets
        write for this window was verified.
        """
        checkpoint_data = {
            "last_window": last_window.isoformat(),
            "atr_state": atr_state,
            "saved_at": datetime.now(tz=IST).isoformat(),
            "sheets_write_confirmed": sheets_write_confirmed,
        }

        # Rotate existing checkpoints
        self._rotate_checkpoints()

        # Atomic write: temp file → rename
        checkpoint_path = self.checkpoint_path
        temp_fd = None

        try:
            temp_fd, temp_path = tempfile.mkstemp(
                dir=str(self._dir), suffix=".tmp"
            )

            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                json.dump(checkpoint_data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            temp_fd = None  # Ownership transferred to fdopen

            # Atomic rename (on same filesystem)
            shutil.move(temp_path, str(checkpoint_path))

            logger.info(
                f"CHECKPOINT_SAVED | window={last_window.time()} | "
                f"tickers={len(atr_state)} | confirmed={sheets_write_confirmed}"
            )

        except Exception as e:
            logger.error(f"CHECKPOINT_SAVE_FAILED | error={e}")
            # Clean up temp file if rename failed
            if temp_fd is not None:
                os.close(temp_fd)
            try:
                if 'temp_path' in locals():
                    os.unlink(temp_path)
            except OSError:
                pass
            raise

    def load_checkpoint(self) -> Optional[dict]:
        """
        Load the most recent valid checkpoint.

        Tries the primary checkpoint first, then falls back to rotated copies.

        Returns:
            Checkpoint dict with keys: last_window, atr_state, saved_at, sheets_write_confirmed
            Returns None if no valid checkpoint exists.
        """
        # Try primary
        data = self._try_load(self.checkpoint_path)
        if data is not None:
            return data

        # Try rotated backups
        for i in range(1, MAX_CHECKPOINT_FILES + 1):
            backup_path = self._dir / f"checkpoint_{i}.json"
            data = self._try_load(backup_path)
            if data is not None:
                logger.warning(
                    f"CHECKPOINT_FALLBACK | primary_corrupt=True | "
                    f"loaded_from=checkpoint_{i}.json"
                )
                return data

        logger.info("NO_CHECKPOINT_FOUND | starting fresh")
        return None

    def _try_load(self, path: Path) -> Optional[dict]:
        """Attempt to load and validate a checkpoint file."""
        if not path.exists():
            return None

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Validate required fields
            if "last_window" not in data or "atr_state" not in data:
                logger.warning(f"CHECKPOINT_INVALID | path={path} | missing_fields")
                return None

            logger.info(
                f"CHECKPOINT_LOADED | path={path.name} | "
                f"window={data['last_window']} | "
                f"tickers={len(data.get('atr_state', {}))}"
            )
            return data

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(f"CHECKPOINT_CORRUPT | path={path} | error={e}")
            return None

    def _rotate_checkpoints(self) -> None:
        """
        Rotate checkpoint files: checkpoint.json → checkpoint_1.json → ...

        Keeps at most MAX_CHECKPOINT_FILES copies.
        """
        # Delete oldest
        oldest = self._dir / f"checkpoint_{MAX_CHECKPOINT_FILES}.json"
        if oldest.exists():
            oldest.unlink()

        # Shift existing backups
        for i in range(MAX_CHECKPOINT_FILES - 1, 0, -1):
            src = self._dir / f"checkpoint_{i}.json"
            dst = self._dir / f"checkpoint_{i + 1}.json"
            if src.exists():
                shutil.move(str(src), str(dst))

        # Move current to checkpoint_1
        if self.checkpoint_path.exists():
            dst = self._dir / "checkpoint_1.json"
            shutil.copy2(str(self.checkpoint_path), str(dst))

    def reconcile_state_on_startup(
        self, sheets_client
    ) -> Tuple[dict, str]:
        """
        🔒5 Cross-validate local checkpoint against Google Sheets state.

        Compares:
        1. last_window from checkpoint vs max(timestamp) from Sheets market_data
        2. Per-ticker ATR values from checkpoint vs Sheets atr_state

        Returns:
            Tuple of (reconciled_atr_state, source_label)
            source_label is 'local', 'sheets', or 'consistent'
        """
        local_checkpoint = self.load_checkpoint()

        if local_checkpoint is None:
            # No local state — try to load from Sheets
            sheets_atr = sheets_client.get_last_atr_state()
            if sheets_atr:
                logger.info(
                    "RECONCILIATION | no_local_checkpoint | "
                    f"adopting_sheets_state | tickers={len(sheets_atr)}"
                )
                return self._convert_sheets_state(sheets_atr), "sheets"
            return {}, "fresh"

        local_window = local_checkpoint["last_window"]
        local_atr = local_checkpoint["atr_state"]
        write_confirmed = local_checkpoint.get("sheets_write_confirmed", True)

        # Get Sheets state
        sheets_last_window = sheets_client.get_last_window_from_sheets()
        sheets_atr_state = sheets_client.get_last_atr_state()

        # Case A: No Sheets data — use local
        if sheets_last_window is None:
            logger.info("RECONCILIATION | no_sheets_data | using_local")
            return local_atr, "local"

        # Compare windows
        if local_window == sheets_last_window:
            # Case B: Consistent
            divergences = self._count_divergences(local_atr, sheets_atr_state)
            logger.info(
                f"RECONCILIATION_CONSISTENT | window={local_window} | "
                f"divergences={divergences}"
            )
            return local_atr, "consistent"

        elif local_window > sheets_last_window:
            # Case C: Local is ahead — checkpoint saved but Sheets write failed
            if not write_confirmed:
                logger.warning(
                    f"RECONCILIATION_LOCAL_AHEAD | "
                    f"local_window={local_window} | "
                    f"sheets_window={sheets_last_window} | "
                    f"write_confirmed=False | using_local_state"
                )
            else:
                logger.warning(
                    f"RECONCILIATION_LOCAL_AHEAD | "
                    f"local_window={local_window} | "
                    f"sheets_window={sheets_last_window} | "
                    f"using_local_state"
                )
            return local_atr, "local"

        else:
            # Case D: Sheets is ahead — fallback flush happened after checkpoint
            logger.warning(
                f"RECONCILIATION_SHEETS_AHEAD | "
                f"local_window={local_window} | "
                f"sheets_window={sheets_last_window} | "
                f"adopting_sheets_state"
            )
            return self._convert_sheets_state(sheets_atr_state), "sheets"

    def _convert_sheets_state(self, sheets_atr: Dict[str, dict]) -> dict:
        """Convert Sheets atr_state format to checkpoint atr_state format."""
        result = {}
        for ticker, state in sheets_atr.items():
            result[ticker] = {
                "prev_close": state.get("last_close"),
                "prev_atr": state.get("last_atr"),
                "tr_history": [],
                "candle_count": 0,  # Unknown from Sheets
            }
        return result

    def _count_divergences(
        self, local_atr: dict, sheets_atr: Dict[str, dict]
    ) -> int:
        """Count ATR value divergences between local and Sheets state."""
        epsilon = 0.0001
        divergences = 0

        for ticker, local_state in local_atr.items():
            sheets_state = sheets_atr.get(ticker)
            if sheets_state is None:
                continue

            local_val = local_state.get("prev_atr")
            sheets_val = sheets_state.get("last_atr")

            if local_val is not None and sheets_val is not None:
                if abs(local_val - sheets_val) > epsilon:
                    logger.warning(
                        f"ATR_DIVERGENCE | ticker={ticker} | "
                        f"local={local_val} | sheets={sheets_val} | "
                        f"diff={abs(local_val - sheets_val):.6f}"
                    )
                    divergences += 1

        return divergences
