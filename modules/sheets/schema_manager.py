"""
Google Sheets Schema Manager — Sheet Initialization and Validation

Creates and validates the 4-sheet schema defined in 03_ULTRA_GOOGLE_SHEETS_SCHEMA.md:
- market_data: OHLC + TR + ATR rows (append-only)
- atr_state: Per-ticker ATR state (overwrite per cycle)
- system_log: System event log (append-only)
- metadata: System configuration (static)
"""

from typing import List

from modules.sheets.sheets_client import SheetsClient
from utils.logger import get_logger

logger = get_logger("sheets.schema_manager")


# Sheet definitions
MARKET_DATA_HEADERS = [
    "id", "timestamp", "ticker", "segment",
    "open", "high", "low", "close",
    "tr", "atr", "volume", "created_at",
]

ATR_STATE_HEADERS = [
    "ticker", "last_close", "last_atr", "last_timestamp", "updated_at",
]

SYSTEM_LOG_HEADERS = [
    "timestamp", "level", "event", "window", "details",
]

METADATA_HEADERS = [
    "key", "value",
]

METADATA_ROWS = [
    ["schema_version", "1.0"],
    ["atr_period", "14"],
    ["timezone", "IST"],
    ["tickers_count", "178"],
]


class SchemaManager:
    """
    Manages Google Sheets schema lifecycle.

    Responsibilities:
    - Initialize sheets with headers if empty
    - Validate existing schema matches expected columns
    - Log system events to system_log sheet
    """

    def __init__(self, sheets_client: SheetsClient):
        self._client = sheets_client

    def initialize_if_empty(self) -> None:
        """
        Create all 4 sheets with headers if they are empty.

        Safe to call multiple times — only writes headers if sheet has no data.
        """
        self._init_sheet("market_data", MARKET_DATA_HEADERS)
        self._init_sheet("atr_state", ATR_STATE_HEADERS)
        self._init_sheet("system_log", SYSTEM_LOG_HEADERS)
        self._init_sheet_with_data("metadata", METADATA_HEADERS, METADATA_ROWS)

        logger.info("SCHEMA_INITIALIZED | sheets=4")

    def _init_sheet(self, sheet_name: str, headers: List[str]) -> None:
        """Initialize a sheet with headers if empty."""
        worksheet = self._client.get_sheet(sheet_name)

        # Check if headers already exist
        existing = worksheet.row_values(1)
        if existing:
            logger.debug(f"SHEET_EXISTS | name={sheet_name} | headers_present=True")
            return

        worksheet.append_row(headers, value_input_option="RAW")
        logger.info(f"HEADERS_WRITTEN | sheet={sheet_name} | columns={len(headers)}")

    def _init_sheet_with_data(
        self, sheet_name: str, headers: List[str], data_rows: List[List[str]]
    ) -> None:
        """Initialize a sheet with headers and initial data rows."""
        worksheet = self._client.get_sheet(sheet_name)

        existing = worksheet.row_values(1)
        if existing:
            logger.debug(f"SHEET_EXISTS | name={sheet_name} | headers_present=True")
            return

        # Write headers + data in one batch
        all_rows = [headers] + data_rows
        worksheet.append_rows(all_rows, value_input_option="RAW")
        logger.info(
            f"SHEET_INITIALIZED | name={sheet_name} | "
            f"headers={len(headers)} | data_rows={len(data_rows)}"
        )

    def validate_schema(self) -> bool:
        """
        Verify that all sheets have correct header columns.

        Returns True if all schemas match, False otherwise.
        """
        valid = True

        for sheet_name, expected_headers in [
            ("market_data", MARKET_DATA_HEADERS),
            ("atr_state", ATR_STATE_HEADERS),
            ("system_log", SYSTEM_LOG_HEADERS),
            ("metadata", METADATA_HEADERS),
        ]:
            worksheet = self._client.get_sheet(sheet_name)
            actual_headers = worksheet.row_values(1)

            if actual_headers != expected_headers:
                logger.error(
                    f"SCHEMA_MISMATCH | sheet={sheet_name} | "
                    f"expected={expected_headers} | actual={actual_headers}"
                )
                valid = False
            else:
                logger.debug(f"SCHEMA_VALID | sheet={sheet_name}")

        if valid:
            logger.info("SCHEMA_VALIDATION_PASSED | all_sheets_valid=True")
        else:
            logger.error("SCHEMA_VALIDATION_FAILED | see errors above")

        return valid

    def log_event(
        self, level: str, event: str, window: str = "", details: str = ""
    ) -> None:
        """
        Append a structured event to the system_log sheet.

        Args:
            level: INFO, WARNING, ERROR
            event: Event identifier, e.g., CANDLE_WRITTEN, RECONNECT
            window: Window timestamp string (optional)
            details: Additional details (optional)
        """
        from utils.time_utils import get_current_ist

        row = [
            get_current_ist().isoformat(),
            level,
            event,
            window,
            details,
        ]

        try:
            worksheet = self._client.get_sheet("system_log")
            worksheet.append_row(row, value_input_option="RAW")
        except Exception as e:
            # Don't let logging failures crash the system
            logger.error(f"SYSTEM_LOG_WRITE_FAILED | event={event} | error={e}")
