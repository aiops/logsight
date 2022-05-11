from abc import ABC, abstractmethod
from typing import List

from connectors.base.connector import Connector


class Source(Connector, ABC):
    """Abstract class depicting source of data. Every data source should implement a method for receiving
        and processing messages."""

    @abstractmethod
    def close(self):
        raise NotImplementedError

    @abstractmethod
    def connect(self):
        raise NotImplementedError

    def has_next(self):
        """Whether the source has a next message."""
        return True

    def receive_message(self):
        """Receive the message from the source/"""
        raise NotImplementedError

    def to_json(self):
        return {}


class StreamSource(Source):
    """Abstract class depicting source of data. Every data source should implement a method for receiving
        and processing messages."""

    @abstractmethod
    def close(self):
        raise NotImplementedError

    @abstractmethod
    def connect(self):
        raise NotImplementedError

    def has_next(self):
        """Whether the source has a next message."""
        return True

    def receive_message(self):
        """Receive the message from the source/"""
        raise NotImplementedError

    def to_json(self):
        return {}


class BatchSource(Source):
    """Abstract class depicting source of data. Every data source should implement a method for receiving
        and processing messages."""

    @abstractmethod
    def close(self):
        raise NotImplementedError

    @abstractmethod
    def connect(self):
        raise NotImplementedError

    def has_next(self):
        """Whether the source has a next message."""
        return True

    def receive_message(self):
        """Receive the message from the source/"""
        raise NotImplementedError

    def to_json(self):
        return {}


class MultiSource(ABC):
    """Abstract class depicting source of data. Every data source should implement a method for receiving
        and processing messages."""

    def __init__(self, sources: List[Source]):
        self.sources = sources

    @abstractmethod
    def connect(self):
        raise NotImplementedError

    @abstractmethod
    def receive_message(self):
        """Receive the message from the source/"""
        return [source.receive_message() for source in self.sources]
