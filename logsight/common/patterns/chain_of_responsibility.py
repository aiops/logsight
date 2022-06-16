from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import List, Optional

from analytics_core.logs import LogBatch

logger = logging.getLogger("logsight." + __name__)


class Handler(ABC):
    """
    The Handler interface declares a method for building the chain of handlers.
    It also declares a method for executing a request.
    """

    @abstractmethod
    def set_next(self, handler: Handler) -> Handler:
        """Set the next handler for handling the event."""
        raise NotImplementedError

    @abstractmethod
    def handle(self, context: Optional[LogBatch]) -> Optional[LogBatch]:
        """ This function handles the incoming context"""
        raise NotImplementedError


class AbstractHandler(Handler):
    """ Abstract implementation of the Handler class"""

    def __init__(self):
        super().__init__()
        self._next_handler: Optional[Handler] = None
        self._ctx = {}

    def set_next(self, handler: Handler) -> Handler:
        """Set the next handler for handling the event.
         Returning a handler from here will let us link handlers in a convenient way like this:
        `handler.set_next(next_handler).set_next(next_next_handler)`
        """
        self._next_handler = handler
        return handler

    def handle(self, context: Optional[LogBatch]) -> Optional[LogBatch]:
        """
        Handles the given context and passes the result to the next handler.
        Args:
            context (Optional[LogBatch]): Context that needs to be handled

        Returns:
            Optional[LogBatch] - The processed context
        """
        if context:
            result = self._handle(context)
            if self._next_handler:
                return self._next_handler.handle(result)
            else:
                return result
        return context

    @abstractmethod
    def _handle(self, context: LogBatch) -> LogBatch:
        """Private method which should contain the processing logic of the context."""
        raise NotImplementedError

    @property
    def next_handler(self):
        return self._next_handler

    def close(self):
        pass


class ForkHandler(AbstractHandler, ABC):
    """This class allows multiple handlers to be attached simultaneously."""

    def __init__(self):
        super().__init__()
        self._next_handler: List[Handler] = list()

    def set_next(self, handler: Handler) -> Handler:
        self._next_handler.append(handler)
        return handler

    def handle(self, context: Optional[LogBatch]) -> Optional[List[LogBatch]]:
        if context and self._next_handler:
            return [_handler.handle(context) for _handler in self._next_handler]
        return context
