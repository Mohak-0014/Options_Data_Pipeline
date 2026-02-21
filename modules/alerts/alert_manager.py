"""
Alert Manager — Multi-Channel Alert Dispatcher

Dispatches system alerts to both the application log and the Google Sheets log.
Handles CRITICAL, WARNING, and INFO events specifically aimed at operator visibility.

🔒9: Reconnect Backoff with Alerting
"""

import threading
from modules.sheets.schema_manager import SchemaManager
from utils.logger import get_logger

logger = get_logger("alerts.alert_manager")

class AlertManager:
    """
    Manages structured alerts across dual channels:
    A) Application logs
    B) Google Sheets system_log table
    """
    def __init__(self, schema_manager: SchemaManager):
        self._schema_manager = schema_manager
        self._lock = threading.Lock()

    def fire(self, severity: str, payload: dict) -> None:
        """
        Fire an alert into both log and Sheet channels.
        Failures in the Sheet channel do not suppress the log channel.

        Args:
            severity: "CRITICAL", "WARNING", "INFO"
            payload: Dict containing at least 'event', 'timestamp', and 'attempt'
        """
        event_name = payload.get("event", "UNKNOWN_EVENT")
        
        # Build log string: "key=value | key=value"
        details_list = []
        for k, v in payload.items():
            if k != "event":
                details_list.append(f"{k}={v}")
        
        details_str = " | ".join(details_list)
        log_msg = f"ALERT | severity={severity} | event={event_name} | {details_str}"

        # Channel A: Logging
        if severity == "CRITICAL":
            logger.critical(log_msg)
        elif severity == "WARNING":
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

        # Channel B: Sheets (Protected)
        sheets_details_str = ",".join(details_list)
        try:
            with self._lock:
                self._schema_manager.log_event(
                    severity=severity,
                    event_type=event_name,
                    details=sheets_details_str
                )
        except Exception as e:
            logger.error(f"ALERT_SHEETS_FAIL | event={event_name} | error={e}")
