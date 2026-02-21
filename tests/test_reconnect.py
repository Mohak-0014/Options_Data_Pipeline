import pytest
from unittest.mock import MagicMock
from modules.websocket.reconnect_manager import ReconnectManager

def test_reconnect_success_first_try(monkeypatch):
    mock_sleep = MagicMock()
    monkeypatch.setattr("time.sleep", mock_sleep)
    
    mock_alert_cb = MagicMock()
    mock_connect = MagicMock()
    mock_subscribe = MagicMock()
    mock_refresh = MagicMock()
    
    mgr = ReconnectManager(
        base_delay_s=2.0, max_delay_s=120.0, backoff_factor=2.0,
        max_attempts=3, jitter=False, alert_callback=mock_alert_cb, alert_threshold=2
    )
    
    success = mgr.attempt_reconnect(mock_connect, mock_subscribe, mock_refresh)
    
    assert success is True
    # Initial sleep of base delay before the first attempt
    mock_sleep.assert_called_once_with(2.0)
    mock_refresh.assert_called_once()
    mock_connect.assert_called_once()
    mock_subscribe.assert_called_once()
    
    # No alerts on first try success
    mock_alert_cb.assert_not_called()
    assert mgr._attempts == 0  # reset was called

def test_reconnect_success_retry_2_and_delay_escalation(monkeypatch):
    mock_sleep = MagicMock()
    monkeypatch.setattr("time.sleep", mock_sleep)
    
    mock_alert_cb = MagicMock()
    
    # Fails first time, succeeds second time
    mock_connect = MagicMock(side_effect=[Exception("Network Error"), None])
    mock_subscribe = MagicMock()
    mock_refresh = MagicMock()
    
    mgr = ReconnectManager(
        base_delay_s=2.0, max_delay_s=120.0, backoff_factor=2.0,
        max_attempts=5, jitter=False, alert_callback=mock_alert_cb, alert_threshold=3
    )
    
    success = mgr.attempt_reconnect(mock_connect, mock_subscribe, mock_refresh)
    
    assert success is True
    assert mock_sleep.call_count == 2
    
    # First sleep: 2.0s
    assert mock_sleep.call_args_list[0][0][0] == 2.0
    # Second sleep: 2 * 2.0 = 4.0s
    assert mock_sleep.call_args_list[1][0][0] == 4.0
    
    # Expected alerts: Warning on first fail, Info on recovery
    assert mock_alert_cb.call_count == 2
    
    call_1 = mock_alert_cb.call_args_list[0]
    assert call_1[0][0] == "WARNING"
    assert call_1[0][1]["event"] == "RECONNECT_ATTEMPT"
    
    call_2 = mock_alert_cb.call_args_list[1]
    assert call_2[0][0] == "INFO"
    assert call_2[0][1]["event"] == "RECONNECT_RECOVERED"
    assert call_2[0][1]["attempts_taken"] == 2
    
    assert mgr._attempts == 0  # reset was called

def test_reconnect_exhaustion_and_threshold_alert(monkeypatch):
    mock_sleep = MagicMock()
    monkeypatch.setattr("time.sleep", mock_sleep)
    
    mock_alert_cb = MagicMock()
    
    # Always fails
    mock_connect = MagicMock(side_effect=Exception("Timeout"))
    mock_subscribe = MagicMock()
    mock_refresh = MagicMock()
    
    mgr = ReconnectManager(
        base_delay_s=2.0, max_delay_s=120.0, backoff_factor=2.0,
        max_attempts=3, jitter=False, alert_callback=mock_alert_cb, alert_threshold=2
    )
    
    success = mgr.attempt_reconnect(mock_connect, mock_subscribe, mock_refresh)
    
    assert success is False
    assert mock_sleep.call_count == 3
    
    # Alerts: 
    # Attempt 1 failed -> WARNING
    # Attempt 2 failed (threshold reached) -> CRITICAL
    # Attempt 3 failed -> CRITICAL
    # Exhaustion -> CRITICAL
    assert mock_alert_cb.call_count == 4
    
    assert mock_alert_cb.call_args_list[0][0][0] == "WARNING"
    assert mock_alert_cb.call_args_list[1][0][0] == "CRITICAL"
    assert mock_alert_cb.call_args_list[2][0][0] == "CRITICAL"
    
    # The final failure is exhaustion
    last_call = mock_alert_cb.call_args_list[3]
    assert last_call[0][0] == "CRITICAL"
    assert last_call[0][1]["event"] == "RECONNECT_EXHAUSTED"
