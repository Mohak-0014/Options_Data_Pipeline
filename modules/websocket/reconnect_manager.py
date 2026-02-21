"""
Reconnect Manager — Stateful WebSocket Reconnect Operator

Manages backoff delay, jitter, attempt counting, and escalating alerts
when the WebSocket connection fails or the API rate limits us.

🔒9: Reconnect Backoff with Alerting
"""

import random
import time
from typing import Callable
from utils.time_utils import get_current_ist

class ReconnectManager:
    """
    Executes a reconnect cycle with exponential backoff.
    
    Alerts:
    - First failure: WARNING
    - Sustained failure (>= threshold): CRITICAL
    - Recovery: INFO
    - Exhaustion: CRITICAL
    """
    
    def __init__(
        self,
        base_delay_s: float,
        max_delay_s: float,
        backoff_factor: float,
        max_attempts: int,
        jitter: bool,
        alert_callback: Callable[[str, dict], None],
        alert_threshold: int = 3,
    ):
        self._base_delay_s = base_delay_s
        self._max_delay_s = max_delay_s
        self._backoff_factor = backoff_factor
        self._max_attempts = max_attempts
        self._jitter = jitter
        self._alert_callback = alert_callback
        self._alert_threshold = alert_threshold
        
        self._attempts = 0

    def attempt_reconnect(
        self,
        connect_fn: Callable[[], None],
        subscribe_fn: Callable[[], None],
        refresh_fn: Callable[[], None],
    ) -> bool:
        """
        Execute one full reconnect cycle with backoff delay.
        Calls refresh_fn -> connect_fn -> subscribe_fn.
        """
        while self._attempts < self._max_attempts:
            delay = min(self._base_delay_s * (self._backoff_factor ** self._attempts), self._max_delay_s)
            if self._jitter:
                delay *= random.uniform(0.75, 1.25)

            time.sleep(delay)
            self._attempts += 1

            try:
                refresh_fn()
                connect_fn()
                subscribe_fn()
                
                if self._attempts > 1:
                    self._alert_callback("INFO", {
                        "event": "RECONNECT_RECOVERED",
                        "timestamp": get_current_ist().isoformat(),
                        "attempt": self._attempts,
                        "attempts_taken": self._attempts
                    })
                
                self.reset()
                return True

            except Exception as e:
                if self._attempts == 1:
                    self._alert_callback("WARNING", {
                        "event": "RECONNECT_ATTEMPT",
                        "timestamp": get_current_ist().isoformat(),
                        "attempt": self._attempts,
                        "error": str(e)
                    })
                elif self._attempts >= self._alert_threshold:
                    self._alert_callback("CRITICAL", {
                        "event": "RECONNECT_FAILING",
                        "timestamp": get_current_ist().isoformat(),
                        "attempt": self._attempts,
                        "error": str(e)
                    })

        self._alert_callback("CRITICAL", {
            "event": "RECONNECT_EXHAUSTED",
            "timestamp": get_current_ist().isoformat(),
            "attempt": self._attempts
        })
        return False

    def reset(self) -> None:
        """Reset the internal attempt counter after a successful connection."""
        self._attempts = 0
