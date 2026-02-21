"""
Google Sheets Client — Authentication, Sheet Access, and Reconciliation

Provides authenticated access to Google Sheets API with connection caching.

🔒3: get_existing_ids_for_window() enables restart-safe ID-based deduplication.
🔒5: get_last_atr_state() enables startup cross-validation.
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Set

import gspread
from google.oauth2.service_account import Credentials

from config.settings import GOOGLE_CREDS_PATH, IST, SPREADSHEET_ID
from utils.logger import get_logger

logger = get_logger("sheets.sheets_client")

# Google Sheets API scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class SheetsClient:
    """
    Google Sheets API client wrapper with caching and reconciliation.

    Provides:
    - Authenticated access via service account
    - Sheet handle caching
    - Monthly spreadsheet management
    - ID-based reconciliation for deduplication
    - ATR state reading for cross-validation
    """

    def __init__(self, creds_path: Optional[str] = None, spreadsheet_id: Optional[str] = None):
        self._creds_path = creds_path or GOOGLE_CREDS_PATH
        self._spreadsheet_id = spreadsheet_id or SPREADSHEET_ID
        self._gc: Optional[gspread.Client] = None
        self._spreadsheet = None
        self._sheet_cache: Dict[str, gspread.Worksheet] = {}

    def _ensure_authenticated(self) -> None:
        """Authenticate with Google Sheets API if not already."""
        if self._gc is not None:
            return

        creds = Credentials.from_service_account_file(
            self._creds_path, scopes=SCOPES
        )
        self._gc = gspread.authorize(creds)
        logger.info("SHEETS_AUTHENTICATED")

    def get_spreadsheet(self, spreadsheet_id: Optional[str] = None):
        """Get or cache the spreadsheet handle."""
        self._ensure_authenticated()

        sid = spreadsheet_id or self._spreadsheet_id
        if self._spreadsheet is None or (spreadsheet_id and spreadsheet_id != self._spreadsheet_id):
            self._spreadsheet = self._gc.open_by_key(sid)
            self._sheet_cache.clear()
            logger.info(f"SPREADSHEET_OPENED | id={sid}")

        return self._spreadsheet

    def get_sheet(self, sheet_name: str) -> gspread.Worksheet:
        """
        Get a worksheet by name, with caching.

        Creates the worksheet if it doesn't exist.
        """
        if sheet_name in self._sheet_cache:
            return self._sheet_cache[sheet_name]

        spreadsheet = self.get_spreadsheet()

        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name, rows=1000, cols=20
            )
            logger.info(f"SHEET_CREATED | name={sheet_name}")

        self._sheet_cache[sheet_name] = worksheet
        return worksheet

    def get_or_create_monthly_spreadsheet(self, year: int, month: int):
        """
        Get or create a monthly spreadsheet: Kotak_Volatility_YYYY_MM

        Returns the spreadsheet handle.
        """
        self._ensure_authenticated()
        name = f"Kotak_Volatility_{year:04d}_{month:02d}"

        try:
            spreadsheet = self._gc.open(name)
            logger.info(f"MONTHLY_SPREADSHEET_FOUND | name={name}")
        except gspread.exceptions.SpreadsheetNotFound:
            spreadsheet = self._gc.create(name)
            logger.info(f"MONTHLY_SPREADSHEET_CREATED | name={name}")

        return spreadsheet

    def get_existing_ids_for_window(
        self, window_start: datetime, sheet_name: str = "market_data"
    ) -> Set[str]:
        """
        🔒3 Fetch all row IDs from Sheets that match the given window timestamp.

        Used for restart-safe deduplication: compare expected IDs against
        already-written IDs to determine which rows need to be appended.

        Args:
            window_start: The window start datetime to search for
            sheet_name: Sheet to query (default: market_data)

        Returns:
            Set of row ID strings found in the sheet for this window
        """
        worksheet = self.get_sheet(sheet_name)

        try:
            # Get all values from the ID column (column 1) and timestamp column (column 2)
            all_values = worksheet.get_all_values()

            if not all_values or len(all_values) <= 1:
                return set()

            # Header is row 0
            window_str = window_start.isoformat()
            existing_ids = set()

            for row in all_values[1:]:  # skip header
                if len(row) >= 2 and row[1] == window_str:
                    existing_ids.add(row[0])  # row[0] is the ID column

            logger.debug(
                f"DEDUP_QUERY | window={window_start.time()} | "
                f"found={len(existing_ids)}"
            )
            return existing_ids

        except Exception as e:
            logger.error(f"DEDUP_QUERY_FAILED | error={e}")
            return set()

    def get_last_atr_state(self) -> Dict[str, dict]:
        """
        🔒5 Read the full atr_state sheet for cross-validation with local checkpoint.

        Returns:
            Dict[ticker, {last_close, last_atr, last_timestamp, updated_at}]
        """
        worksheet = self.get_sheet("atr_state")

        try:
            all_values = worksheet.get_all_values()

            if not all_values or len(all_values) <= 1:
                return {}

            # Expected columns: ticker, last_close, last_atr, last_timestamp, updated_at
            state = {}
            for row in all_values[1:]:  # skip header
                if len(row) >= 4:
                    ticker = row[0]
                    state[ticker] = {
                        "last_close": float(row[1]) if row[1] else None,
                        "last_atr": float(row[2]) if row[2] else None,
                        "last_timestamp": row[3] if row[3] else None,
                        "updated_at": row[4] if len(row) > 4 and row[4] else None,
                    }

            logger.info(f"ATR_STATE_FROM_SHEETS | tickers={len(state)}")
            return state

        except Exception as e:
            logger.error(f"ATR_STATE_READ_FAILED | error={e}")
            return {}

    def get_last_window_from_sheets(self) -> Optional[str]:
        """
        🔒5 Get the latest window timestamp from market_data sheet.

        Returns the max timestamp string found, or None if sheet is empty.
        """
        worksheet = self.get_sheet("market_data")

        try:
            all_values = worksheet.get_all_values()
            if not all_values or len(all_values) <= 1:
                return None

            # Timestamp is column 2 (index 1)
            timestamps = [row[1] for row in all_values[1:] if len(row) >= 2 and row[1]]
            if not timestamps:
                return None

            return max(timestamps)

        except Exception as e:
            logger.error(f"LAST_WINDOW_QUERY_FAILED | error={e}")
            return None

    def clear_cache(self) -> None:
        """Clear sheet handle cache (e.g., after monthly rotation)."""
        self._sheet_cache.clear()
