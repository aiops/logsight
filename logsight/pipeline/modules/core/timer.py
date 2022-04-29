from __future__ import annotations

import logging
from threading import Timer

logger = logging.getLogger("logsight." + __name__)


class NamedTimer:
    def __init__(self, timeout_period: int, callback: callable, name: str = ""):
        self.timeout_period = timeout_period
        self.callback = callback
        self.name = name + '_timer'
        self.timer = Timer(timeout_period, callback)
        self.timer.name = self.name

    def start(self) -> 'NamedTimer':
        # logger.debug(f"Starting timer {self.name} {self}")
        self.timer.start()
        return self

    def reset_timer(self) -> 'NamedTimer':
        self.cancel()
        return self.start()

    def cancel(self):
        # logger.debug(f"Cancelling timer {self.name} {self.timer}")
        self.timer.cancel()
        self.timer = Timer(self.timeout_period, self.callback)
        self.timer.name = self.name
        # logger.debug(f"New timer object {self.timer}")


from functools import wraps
from time import time


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print('func:%r %r took: %2.4f sec' % \
              (f.__name__, kw, te - ts))
        return result

    return wrap
