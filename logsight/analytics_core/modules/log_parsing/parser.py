from abc import ABC, abstractmethod


class Parser(ABC):
    """Abstract class for log parsers. Every log parser should implement the parse method."""

    @abstractmethod
    def parse(self, text):
        raise NotImplementedError
