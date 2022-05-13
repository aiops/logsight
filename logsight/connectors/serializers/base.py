from abc import ABC, abstractmethod
from typing import Any


class Serializer(ABC):
    """
    Interface for transforming data from data source.
    """

    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        raise NotImplementedError
