from abc import ABC, abstractmethod


class Sink(ABC):
    """Abstract class depicting source of data. Every data source should implement a method for receiving
        and processing messages."""

    def __init__(self, **kwargs):
        # init
        pass

    @abstractmethod
    def send(self, data):
        raise NotImplementedError

    def connect(self):
        # default behaviour
        pass
