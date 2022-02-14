from abc import ABC, abstractmethod
from typing import List


class Source(ABC):
    """Abstract class depicting source of data. Every data source should implement a method for receiving
        and processing messages."""

    def __init__(self, **kwargs):
        # do nothing
        pass

    @abstractmethod
    def connect(self):
        raise NotImplementedError

    @abstractmethod
    def receive_message(self):
        """Receive the message from the source/"""
        raise NotImplementedError

    def has_next(self):
        """Whether the source has a next message."""
        return True

    def to_json(self):
        return {}


class StreamSource(Source):
    """Abstract class depicting source of data. Every data source should implement a method for receiving
        and processing messages."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @abstractmethod
    def connect(self):
        raise NotImplementedError

    @abstractmethod
    def receive_message(self):
        """Receive the message from the source/"""
        raise NotImplementedError

    def has_next(self):
        """Whether the source has a next message."""
        return True


class BatchSource(Source):
    """Abstract class depicting source of data. Every data source should implement a method for receiving
        and processing messages."""

    def __init__(self):
        super().__init__()

    @abstractmethod
    def connect(self):
        raise NotImplementedError

    @abstractmethod
    def receive_message(self):
        """Receive the message from the source/"""
        raise NotImplementedError

    @abstractmethod
    def process_message(self):
        """Process the message from the source."""
        raise NotImplementedError

    def has_next(self):
        """Whether the source has a next message."""
        return True


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
