"""
🔒4 NSE Trading Calendar — Holiday and Special Session Handler

Prevents the system from operating on non-trading days (weekends, NSE holidays)
and adjusts session hours for special trading sessions (e.g., Muhurat Trading).

Calendar data is loaded from JSON files in data/calendars/holidays_YYYY.json.
NSE publishes the holiday list annually; update the JSON before each new year.
"""

import json
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Optional, Tuple

from config.settings import CALENDAR_DIR, IST, MARKET_CLOSE, MARKET_OPEN


class TradingCalendar:
    """NSE trading calendar with holiday and special session awareness."""

    def __init__(self, calendar_dir: Optional[Path] = None):
        self._calendar_dir = calendar_dir or CALENDAR_DIR
        self._holidays: dict[int, set[date]] = {}  # year -> set of holiday dates
        self._special_sessions: dict[date, dict] = {}  # date -> session info
        self._loaded_years: set[int] = set()

    def _ensure_year_loaded(self, year: int) -> None:
        """Load calendar data for the given year if not already loaded."""
        if year in self._loaded_years:
            return

        calendar_file = self._calendar_dir / f"holidays_{year}.json"
        if not calendar_file.exists():
            # No calendar file — treat all weekdays as trading days
            self._holidays[year] = set()
            self._loaded_years.add(year)
            return

        with open(calendar_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Parse holidays
        holidays = set()
        for entry in data.get("holidays", []):
            holidays.add(date.fromisoformat(entry["date"]))
        self._holidays[year] = holidays

        # Parse special sessions
        for entry in data.get("special_sessions", []):
            session_date = date.fromisoformat(entry["date"])
            self._special_sessions[session_date] = {
                "name": entry["name"],
                "open": time.fromisoformat(entry["open"]),
                "close": time.fromisoformat(entry["close"]),
            }

        self._loaded_years.add(year)

    def is_trading_day(self, check_date: date) -> bool:
        """
        Returns True if the given date is a trading day.
        Returns False for weekends and NSE holidays.
        Special sessions (e.g., Muhurat trading on a holiday) ARE trading days.
        """
        self._ensure_year_loaded(check_date.year)

        # Saturday = 5, Sunday = 6
        if check_date.weekday() >= 5:
            # Check if there's a special session on this weekend day
            return check_date in self._special_sessions
        
        # Check holidays — but special sessions override
        if check_date in self._holidays.get(check_date.year, set()):
            return check_date in self._special_sessions

        return True

    def get_session_hours(self, check_date: date) -> Tuple[time, time]:
        """
        Returns (open_time, close_time) for the given trading day.
        Returns special session hours if applicable, otherwise default market hours.
        
        Raises ValueError if called on a non-trading day.
        """
        if not self.is_trading_day(check_date):
            raise ValueError(f"{check_date} is not a trading day")

        self._ensure_year_loaded(check_date.year)

        if check_date in self._special_sessions:
            session = self._special_sessions[check_date]
            return session["open"], session["close"]

        return MARKET_OPEN, MARKET_CLOSE

    def get_next_trading_day(self, from_date: date) -> date:
        """
        Returns the next trading day strictly after from_date.
        Skips weekends and holidays.
        """
        candidate = from_date + timedelta(days=1)
        # Safety: don't loop more than 30 days (handles year boundaries)
        for _ in range(30):
            self._ensure_year_loaded(candidate.year)
            if self.is_trading_day(candidate):
                return candidate
            candidate += timedelta(days=1)

        # Fallback: return next Monday if something is very wrong
        raise RuntimeError(
            f"Could not find a trading day within 30 days of {from_date}. "
            "Check calendar file."
        )

    def get_holiday_name(self, check_date: date) -> Optional[str]:
        """Returns the holiday name if the date is a holiday, else None."""
        self._ensure_year_loaded(check_date.year)

        if check_date in self._holidays.get(check_date.year, set()):
            # Find the name from the calendar file
            calendar_file = self._calendar_dir / f"holidays_{check_date.year}.json"
            if calendar_file.exists():
                with open(calendar_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for entry in data.get("holidays", []):
                    if date.fromisoformat(entry["date"]) == check_date:
                        return entry["name"]
        return None


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
trading_calendar = TradingCalendar()
