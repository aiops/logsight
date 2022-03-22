from __future__ import annotations

import collections
import copy
import logging
import statistics
from abc import ABC, abstractmethod
from copy import deepcopy
from time import perf_counter
from typing import Any, Optional, List

from modules.core.timer import NamedTimer
from modules.core.wrappers import synchronized

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

    def handle(self, request: Any) -> Optional[str]:
        if request:
            self.stats.receive_request()
            result = self._handel(request)
            self.stats.handled_request()
            if self._next_handler:
                return self._next_handler.handle(result)
            else:
                return result
        return request

    @abstractmethod
    def _handel(self, request) -> Optional[str]:
        raise NotImplementedError

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
            if self.next_handlers:
                return [_handler.handle(request) for _handler in self.next_handlers]
        return request

    @property
    def next_handlers(self):
        return self._next_handlers


class HandlerStats:
    def __init__(self, ctx: dict, log_stats_interval_sec: int = 60):
        self.ctx = ctx
        self.num_request = 0
        self.num_result = 0
        self.request_result_times = collections.deque(maxlen=10000)
        self.perf_counter = 0

        self.log_stats_interval_sec = log_stats_interval_sec

        self.timer = NamedTimer(self.log_stats_interval_sec, self.log_stats, self.__class__.__name__)
        self.timer.start()

    def receive_request(self):
        self.num_request += 1
        self.perf_counter = perf_counter()

    def handled_request(self):
        self.num_result += 1
        perf_counter_request = perf_counter()
        self.request_result_times.append(perf_counter_request - self.perf_counter)

    def log_stats(self):
        mean_processing_time = 0
        if len(self.request_result_times) > 0:
            copy_times = copy.deepcopy(self.request_result_times)
            mean_processing_time = statistics.mean(copy_times)
        logger.debug(f"Handler {self.ctx} handled {self.num_request} requests and calculated {self.num_result} results in total. " +
                     f"Handling of 1000 requests took on average {mean_processing_time * 1000} seconds.")
        self.timer.reset_timer()
