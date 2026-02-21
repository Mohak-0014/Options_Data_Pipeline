"""
Structured Logger — File + Console with Daily Rotation

Provides a centralized logging facility for all modules.
Log format: [timestamp IST] [level] [module] message
"""

import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from config.settings import IST, LOG_DIR, LOG_LEVEL


class ISTFormatter(logging.Formatter):
    """Custom formatter that outputs timestamps in IST."""

    def __init__(self, fmt=None, datefmt=None):
        super().__init__(fmt=fmt, datefmt=datefmt)

    def formatTime(self, record, datefmt=None):
        from datetime import datetime, timezone

        # Convert to IST
        ct = datetime.fromtimestamp(record.created, tz=IST)
        if datefmt:
            return ct.strftime(datefmt)
        return ct.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # ms precision


def get_logger(name: str) -> logging.Logger:
    """
    Get or create a logger with the given name.

    Each logger gets:
    - Console handler (stdout)
    - File handler (daily rotation, kept 30 days)

    Args:
        name: Module name, e.g., 'auth.authenticator' or 'aggregator.tick_buffer'

    Returns:
        Configured logging.Logger instance
    """
    logger = logging.getLogger(name)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    # Format
    fmt = "[%(asctime)s] [%(levelname)-8s] [%(name)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = ISTFormatter(fmt=fmt, datefmt=datefmt)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)

    # File handler — daily rotation
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "volatility_harvester.log"
    file_handler = TimedRotatingFileHandler(
        filename=str(log_file),
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger
