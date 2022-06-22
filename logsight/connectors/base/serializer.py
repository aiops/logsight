from abc import ABC, abstractmethod
from typing import Any


class Serializer(ABC):
    """
    Interface for transforming data from data source.
    """

    @abstractmethod
    def serialize(self, data: Any) -> str:
        """
        The serialize function serialize an object into a string.
        The corresponding object is defined by the implementations of this interface. and returns the corresponding
        Args:
           data:Any: Object that needs to be deserialized

        Returns:
            Serialized object as string
        """
        raise NotImplementedError

    @abstractmethod
    def deserialize(self, data: str) -> Any:
        """
        The deserialize function deserializes a string into a corresponding object defined
        by the implementations of this interface. and returns the corresponding
        Args:
            data:str: String that needs to be deserialized
        Returns:
           Deserialized object
        """
        raise NotImplementedError
