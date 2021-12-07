from __future__ import annotations
from threading import Timer


class NamedTimer:
    def __init__(self, timeout_period: int, callback: callable, name: str = ""):
        self.timeout_period = timeout_period
        self.callback = callback
        self.name = name + '_timer'
        self.timer = Timer(timeout_period, callback)
        self.timer.name = self.name

    def start(self) -> 'NamedTimer':
        self.timer.start()
        return self

    def reset_timer(self) -> 'NamedTimer':
        self.cancel()
        return self.start()

    def cancel(self):
        self.timer.cancel()
        self.timer = Timer(self.timeout_period, self.callback)
        self.timer.name = self.name