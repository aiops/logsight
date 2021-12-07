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


class ForkHandler(Handler):
    _next_handlers: List[Handler] = []

    def set_next(self, handler: Handler) -> Handler:
        self._next_handlers.append(handler)
        return handler

    def handle(self, request) -> Optional[List]:
        if self._next_handlers:
            response = []
            for _handler in self._next_handlers:
                response.append(_handler.handle(request))
            return response
        return None

    @property
    def next_handlers(self):
        return self._next_handlers
