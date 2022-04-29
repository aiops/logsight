from abc import ABC, abstractmethod
from typing import Optional

from connectors.base.connector import Connector
from connectors.transformers import DictTransformer, Transformer


class Source:
    """Abstract class depicting source of data. Every data source should implement a method for receiving messages."""

    def __init__(self, transformer: Optional[Transformer] = None):
        self.transformer = transformer or DictTransformer()

    def has_next(self):
        """Whether the source has a next message."""
        return True

    def _receive_message(self) -> str:
        """
        This function receives a message from the source
        """
        raise NotImplementedError

    def receive_message(self):
        """
        This function receives a message from the source and transforms it
        :return: The transformed message
        """
        msg = self._receive_message()
        return self.transformer.transform(msg)


class ConnectionSource(Source, Connector, ABC):
    """Interface for Source that is also able to connect to endpoint."""


class StreamSource(Source):
    """Abstract class depicting source of data. Every data source should implement a method for receiving
        and processing messages."""

    @abstractmethod
    def connect(self):
        raise NotImplementedError

    def has_next(self):
        """Whether the source has a next message."""
        return True

    def receive_message(self):
        """Receive the message from the source/"""
        raise NotImplementedError
