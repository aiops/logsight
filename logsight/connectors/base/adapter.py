from abc import ABC
from connectors.base.serializer import Serializer
from connectors.base.connector import Connector


class Adapter(ABC):
    """Base interface containing connector and serializer interfaces"""

    def __init__(self, connector: Connector = None, serializer: Serializer = None):
        self.connector = connector
        self.serializer = serializer
