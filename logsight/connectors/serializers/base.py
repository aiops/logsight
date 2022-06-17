from abc import ABC, abstractmethod
from typing import Any

from analytics_core.logs import LogBatch


class Serializer(ABC):
    """
    Interface for transforming data from data source.
    """

    @abstractmethod
    def serialize(self, data: Any) -> str:
        raise NotImplementedError

    @abstractmethod
    def deserialize(self, data: str) -> Any:
        raise NotImplementedError


class LogBatchSerializer(Serializer):
    """
    Interface for serialization of LogBatch objects.
    """

    @abstractmethod
    def serialize(self, data: LogBatch) -> Any:
        raise NotImplementedError

    @abstractmethod
    def deserialize(self, data: Any) -> LogBatch:
        raise NotImplementedError
