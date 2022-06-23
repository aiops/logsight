from abc import ABC

from connectors.base.connectable import Connectable
from connectors.base.connector import Connector
from connectors.base.sink import Sink
from connectors.base.source import Source


class ConnectableConnector(Connector, Connectable, ABC):
    """Interface that allows the connector to connect to an endpoint"""


class ConnectableSink(Sink, ConnectableConnector, ABC):
    """Interface for Sink that is also able to connect to endpoint."""


class ConnectableSource(Source, ConnectableConnector, ABC):
    """Interface for Source that is also able to connect to endpoint."""
