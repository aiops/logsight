from abc import ABC, abstractmethod
from typing import Optional

from analytics_core.logs import LogBatch
from connectors.base.connector import Connector
from connectors.serializers import JSONSerializer
from connectors.serializers.base import LogBatchSerializer


class Source(ABC):
    """Abstract class depicting source of data. Every data source should implement a method for receiving messages."""

    def has_next(self) -> bool:
        """Whether the source has a next message."""
        return True

    @abstractmethod
    def _receive_message(self) -> str:
        """
        This function receives a message from the source
        """
        raise NotImplementedError

    def receive_message(self) -> str:
        """
        This function receives a message from the source and transforms it
        :return: The transformed message
        """
        return self._receive_message()


class ConnectableSource(Source, Connector, ABC):
    """Interface for Source that is also able to connect to endpoint."""


class LogBatchSource(Source, ABC):
    """Interface for Sources of LogBatch objects."""

    def __init__(self, serializer: Optional[LogBatchSerializer] = None):
        self.serializer = serializer or JSONSerializer()

    def receive_message(self) -> LogBatch:
        """
        This function receives a message from the source and deserializes a LogBatch out of it
        :return: deserialized LogBatch
        """
        msg = super().receive_message()
        return self.serializer.deserialize(msg)


class LogBatchConnectableSource(LogBatchSource, Connector, ABC):
    """Interface for Sources of LogBatch objects that is also able to connect to endpoint."""


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
