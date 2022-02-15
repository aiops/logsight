from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any, Optional, List

from modules.core.timer import NamedTimer

logger = logging.getLogger("logsight." + __name__)


class Handler(ABC):
    """
    The Handler interface declares a method for building the chain of handlers.
    It also declares a method for executing a request.
    """

    def __init__(self):
        self.stats = None

    @abstractmethod
    def set_next(self, handler: Handler) -> Handler:
        raise NotImplementedError

    @abstractmethod
    def handle(self, request) -> Optional[str]:
        raise NotImplementedError

    def start(self, ctx: dict):
        raise NotImplementedError

    @abstractmethod
    def flush(self, context: Optional[Any]) -> Optional[str]:
        raise NotImplementedError


class AbstractHandler(Handler):

    def __init__(self):
        super().__init__()
        self._next_handler: Optional[Handler] = None
        self._ctx = {}

    def set_next(self, handler: Handler) -> Handler:
        self._next_handler = handler
        # Returning a handler from here will let us link handlers in a
        # convenient way like this:
        # handler.set_next(next_handler).set_next(next_next_handler)
        return handler

    @abstractmethod
    def handle(self, request: Any) -> Optional[str]:
        if request:
            self.stats.incr_stats()
            if self._next_handler:
                return self._next_handler.handle(request)
        return request

    @property
    def next_handler(self):
        return self._next_handler

    def start(self, ctx: dict):
        self._ctx = deepcopy(ctx)
        self.stats = HandlerStats(self._ctx)
        if self.next_handler:
            self.next_handler.start(ctx)

    @abstractmethod
    def flush(self, context: Optional[Any]) -> Optional[str]:
        if self._next_handler:
            return self._next_handler.flush(context)


class ForkHandler(Handler):
    def flush(self, context: Optional[Any]) -> Optional[List]:
        self.stats.incr_stats()
        if self.next_handlers:
            return [_handler.flush(context) for _handler in self.next_handlers]

    def __init__(self):
        super().__init__()
        self._next_handlers: List[Handler] = []
        self._ctx = {}

    def start(self, ctx: dict):
        self._ctx = deepcopy(ctx)
        self.stats = HandlerStats(self._ctx)
        if self.next_handlers:
            for _handler in self.next_handlers:
                _handler.start(ctx)

    def set_next(self, handler: Handler) -> Handler:
        self.next_handlers.append(handler)
        return handler

    def handle(self, request) -> Optional[List]:
        if request:
            self.stats.incr_stats()
            if self.next_handlers:
                return [_handler.handle(request) for _handler in self.next_handlers]
        return None

    @property
    def next_handlers(self):
        return self._next_handlers


class HandlerStats:
    def __init__(self, ctx: dict, log_stats_interval_sec: int = 60):
        self.ctx = ctx
        self.num_handled_total = 0
        self.num_handled = 0
        self.log_stats_interval_sec = log_stats_interval_sec

        self.timer = NamedTimer(self.log_stats_interval_sec, self.log_stats, self.__class__.__name__)
        self.timer.start()

    def incr_stats(self):
        self.num_handled += 1
        self.num_handled_total += 1

    def log_stats(self):
        freq = float(self.num_handled) / float(self.log_stats_interval_sec)
        logger.debug(f"Handler {self.ctx} handled {self.num_handled_total} messages in total. " +
                     f"Handling frequency during last {self.log_stats_interval_sec} seconds: {freq}")
        self.num_handled = 0
        self.timer.reset_timer()
