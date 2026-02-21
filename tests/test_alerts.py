import pytest
from unittest.mock import MagicMock, patch
from modules.alerts.alert_manager import AlertManager

@patch("modules.alerts.alert_manager.logger")
def test_alert_manager_log_channel(mock_logger):
    mock_schema_manager = MagicMock()
    alert_mgr = AlertManager(mock_schema_manager)
    
    alert_mgr.fire("WARNING", {"event": "TEST_WARN", "timestamp": "123", "attempt": 1})
    
    mock_logger.warning.assert_called_once_with(
        "ALERT | severity=WARNING | event=TEST_WARN | timestamp=123 | attempt=1"
    )
    
    # Verify SchemaManager was called
    mock_schema_manager.log_event.assert_called_once_with(
        severity="WARNING", 
        event_type="TEST_WARN", 
        details="timestamp=123,attempt=1"
    )

@patch("modules.alerts.alert_manager.logger")
def test_alert_manager_sheets_failure_does_not_raise(mock_logger):
    mock_schema_manager = MagicMock()
    mock_schema_manager.log_event.side_effect = Exception("Google API Error")
    
    alert_mgr = AlertManager(mock_schema_manager)
    
    # Should not raise exception
    alert_mgr.fire("CRITICAL", {"event": "TEST_CRIT", "timestamp": "123", "attempt": 3})
        
    mock_logger.critical.assert_called_once_with(
        "ALERT | severity=CRITICAL | event=TEST_CRIT | timestamp=123 | attempt=3"
    )
    mock_logger.error.assert_called_once_with(
        "ALERT_SHEETS_FAIL | event=TEST_CRIT | error=Google API Error"
    )
