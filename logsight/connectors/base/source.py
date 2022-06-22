from abc import ABC, abstractmethod

from connectors.base import Connector


class Source(Connector, ABC):
    """Abstract class depicting source of data. Every data source should implement a method for receiving messages."""

    def has_next(self):
        """Whether the source has a next message."""
        return True

    @abstractmethod
    def receive_message(self) -> str:
        """
        This function receives a message from the source
        """
        raise NotImplementedError
