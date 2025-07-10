import time
from typing import Callable

class Watchdog:
    def __init__(self, timeout: float, on_timeout: Callable):
        self.timeout = timeout
        self.on_timeout = on_timeout
        self._last_reset = time.time()
        self._timeout_triggered = False
        self._enabled = True

    def reset(self):
        self._last_reset = time.time()
        self._timeout_triggered = False

    def stop(self):
        self._enabled = False

    def check(self):
        if not self._enabled:
            return
        if time.time() - self._last_reset > self.timeout:
            if not self._timeout_triggered:
                self._timeout_triggered = True
                self.on_timeout()
