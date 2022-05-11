from abc import ABC, abstractmethod


class Connector(ABC):
    """Base class for connector"""

    @abstractmethod
    def connect(self):
        """Establish connection to endpoint."""
        raise NotImplementedError

    @abstractmethod
    def close(self):
        """Close connection to endpoint."""
        raise NotImplementedError
