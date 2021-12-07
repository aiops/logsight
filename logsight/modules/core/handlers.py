from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Optional, List


class Handler(ABC):
    """
    The Handler interface declares a method for building the chain of handlers.
    It also declares a method for executing a request.
    """

    @abstractmethod
    def set_next(self, handler: Handler) -> Handler:
        raise NotImplementedError

    @abstractmethod
    def handle(self, request) -> Optional[str]:
        raise NotImplementedError

    def start(self):
        raise NotImplementedError


class AbstractHandler(Handler):
    _next_handler: Optional[Handler] = None

    def set_next(self, handler: Handler) -> Handler:
        self._next_handler = handler
        # Returning a handler from here will let us link handlers in a
        # convenient way like this:
        # handler.set_next(next_handler).set_next(next_next_handler)
        return handler

    @abstractmethod
    def handle(self, request: Any) -> Optional[str]:
        if self._next_handler:
            return self._next_handler.handle(request)
        return None

    @property
    def next_handler(self):
        return self._next_handler

    def start(self):
        if self.next_handler:
            self.next_handler.start()


class ForkHandler(Handler):
    def start(self):
        if self._next_handlers:
            for _handler in self._next_handlers:
                _handler.start()

    _next_handlers: List[Handler] = []

    def set_next(self, handler: Handler) -> Handler:
        self._next_handlers.append(handler)
        return handler

    def handle(self, request) -> Optional[List]:
        if self._next_handlers:
            return [_handler.handle(request) for _handler in self._next_handlers]
        return None

    @property
    def next_handlers(self):
        return self._next_handlers
