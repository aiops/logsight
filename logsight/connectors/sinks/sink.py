from abc import ABC, abstractmethod
from typing import Any, Optional

from connectors.base.connector import Connector
from connectors.serializers import DictSerializer, Serializer


class Sink:
    """Abstract class depicting source of data. Every data source should implement a method for receiving
        and processing messages."""

    def __init__(self, serializer: Optional[Serializer] = None):
        self.serializer = serializer or DictSerializer()

    @abstractmethod
    def send(self, data: Any, target: Optional[str] = None):
        raise NotImplementedError


class ConnectableSink(Sink, Connector, ABC):
    """Interface for Sink that is also able to connect to endpoint."""
