from abc import ABC, abstractmethod
from typing import Any, Optional

from .connector import Connector


class Sink(Connector, ABC):
    """ Interface for sending data to target"""

    @abstractmethod
    def send(self, data: Any, target: Optional[Any] = None):
        raise NotImplementedError
