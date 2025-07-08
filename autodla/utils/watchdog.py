import threading
import time
from typing import Callable

class Watchdog:
    def __init__(self, timeout : int, on_timeout : Callable):
        self.timeout = timeout
        self.on_timeout = on_timeout
        self.timer = None
        self.lock = threading.Lock()

    def _start_timer(self):
        self.timer = threading.Timer(self.timeout, self._timeout)
        self.timer.start()

    def _timeout(self):
        with self.lock:
            self.timer = None
        self.on_timeout()

    def reset(self):
        with self.lock:
            if self.timer:
                self.timer.cancel()
            self._start_timer()

    def stop(self):
        with self.lock:
            if self.timer:
                self.timer.cancel()
                self.timer = None