"""
Kotak Neo Authentication Module (v2 SDK)

Auth flow:
  1. NeoAPI(consumer_key=token)
  2. client.totp_login(mobile, ucc, totp)  → generates view token + session ID
  3. client.totp_validate(mpin)             → generates trade token

Uses the Kotak-Neo/Kotak-neo-api-v2 SDK.
Install: pip install "git+https://github.com/Kotak-Neo/Kotak-neo-api-v2.git@v2.0.1#egg=neo_api_client"
"""

import time as time_module
from datetime import datetime
from typing import Optional

import pyotp

from config.settings import (
    IST,
    KOTAK_CONSUMER_KEY,
    KOTAK_MOBILE,
    KOTAK_MPIN,
    KOTAK_UCC,
    SESSION_MAX_AGE_HOURS,
    TOTP_SECRET,
)
from utils.logger import get_logger

logger = get_logger("auth.authenticator")


class AuthenticationFailed(Exception):
    """Raised when all authentication retry attempts are exhausted."""
    pass


class Authenticator:
    """
    Manages Kotak Neo API v2 authentication lifecycle.

    Handles:
    - TOTP generation via pyotp
    - totp_login() with mobile + UCC
    - totp_validate() with MPIN
    - Automatic session refresh on expiry
    - Retry with backoff on failures
    """

    MAX_RETRIES = 3
    RETRY_BACKOFF_S = 5

    def __init__(self):
        self._client = None
        self._session_created_at: Optional[datetime] = None
        self._totp = pyotp.TOTP(TOTP_SECRET) if TOTP_SECRET else None

    def _generate_totp(self) -> str:
        """Generate current TOTP code."""
        if not self._totp:
            raise AuthenticationFailed("TOTP_SECRET not configured")
        code = self._totp.now()
        logger.debug("TOTP generated successfully")
        return code

    def login(self) -> None:
        """
        Execute full v2 login flow:
        1. Generate TOTP
        2. Create NeoAPI client with consumer_key
        3. Call totp_login(mobile, ucc, totp)
        4. Call totp_validate(mpin)
        5. Store session timestamp

        Raises AuthenticationFailed after MAX_RETRIES failures.
        """
        last_error = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                logger.info(f"Authentication attempt {attempt}/{self.MAX_RETRIES}")

                # Generate fresh TOTP
                totp_code = self._generate_totp()

                # Import here to avoid import errors when neo_api_client is not installed
                from neo_api_client import NeoAPI

                # Step 1: Create client with consumer key (token from Neo app)
                client = NeoAPI(
                    environment="prod",
                    access_token=None,
                    neo_fin_key=None,
                    consumer_key=KOTAK_CONSUMER_KEY,
                )

                # Step 2: TOTP login → generates view token + session ID
                client.totp_login(
                    mobile_number=KOTAK_MOBILE,
                    ucc=KOTAK_UCC,
                    totp=totp_code,
                )

                # Step 3: Validate with MPIN → generates trade token
                client.totp_validate(mpin=KOTAK_MPIN)

                # Success
                self._client = client
                self._session_created_at = datetime.now(tz=IST)

                logger.info(
                    f"Authentication successful | "
                    f"session_created={self._session_created_at.isoformat()}"
                )
                return

            except Exception as e:
                last_error = e
                logger.warning(
                    f"Authentication attempt {attempt} failed: {type(e).__name__}: {e}"
                )
                if attempt < self.MAX_RETRIES:
                    logger.info(f"Retrying in {self.RETRY_BACKOFF_S}s...")
                    time_module.sleep(self.RETRY_BACKOFF_S)

        # All retries exhausted
        logger.error(
            f"Authentication failed after {self.MAX_RETRIES} attempts | "
            f"last_error={last_error}"
        )
        raise AuthenticationFailed(
            f"Failed to authenticate after {self.MAX_RETRIES} attempts: {last_error}"
        )

    def get_client(self):
        """
        Returns the authenticated NeoAPI client.

        Auto-refreshes session if expired.
        Raises AuthenticationFailed if not logged in and login fails.
        """
        if self._client is None:
            logger.info("No active session — initiating login")
            self.login()

        if not self.is_session_valid():
            logger.info("Session expired — refreshing")
            self.refresh_session()

        return self._client

    def is_session_valid(self) -> bool:
        """
        Check if the current session is still valid.

        Returns False if:
        - No session exists
        - Session is older than SESSION_MAX_AGE_HOURS
        """
        if self._client is None or self._session_created_at is None:
            return False

        age = datetime.now(tz=IST) - self._session_created_at
        age_hours = age.total_seconds() / 3600

        if age_hours >= SESSION_MAX_AGE_HOURS:
            logger.info(
                f"Session age {age_hours:.1f}h exceeds max {SESSION_MAX_AGE_HOURS}h"
            )
            return False

        return True

    def refresh_session(self) -> None:
        """
        Re-authenticate by performing a fresh login.

        Discards the old client and creates a new one.
        """
        logger.info("Refreshing session — performing full re-login")
        self._client = None
        self._session_created_at = None
        self.login()
