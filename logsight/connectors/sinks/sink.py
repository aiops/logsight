from abc import ABC, abstractmethod
from typing import Any, Optional

from connectors.base.connector import Connector
from connectors.serializers import JSONSerializer
from connectors.serializers import LogBatchSerializer


class Sink:
    """Abstract class depicting source of data. Every data source should implement a method for receiving
        and processing messages."""

    def __init__(self, serializer: Optional[LogBatchSerializer] = None):
        self.serializer = serializer or JSONSerializer()

    @abstractmethod
    def send(self, data: Any, target: Optional[str] = None):
        raise NotImplementedError


class ConnectableSink(Sink, Connector, ABC):
    """Interface for Sink that is also able to connect to endpoint."""
