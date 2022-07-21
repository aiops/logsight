from abc import ABC, abstractmethod

from connectors.base.connector import Connector


class HasNextMixin:
    @staticmethod
    def has_next():
        """Whether the source has a next message."""
        return True


class Source(Connector, HasNextMixin, ABC):
    """Abstract class depicting source of data. Every data source should implement a method for receiving messages."""

    @abstractmethod
    def receive_message(self) -> str:
        """
        This function receives a message from the source
        """
        raise NotImplementedError
