from abc import abstractmethod

from connectors.base.connector import Connector


class Sink(Connector):
    """Abstract class depicting source of data. Every data source should implement a method for receiving
        and processing messages."""

    @abstractmethod
    def send(self, data):
        raise NotImplementedError
