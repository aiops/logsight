from abc import ABC, abstractmethod


class Connector(ABC):
    """Base class for connector"""

    @abstractmethod
    def connect(self):
        # default behaviour
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError

    def to_json(self):
        return {}
