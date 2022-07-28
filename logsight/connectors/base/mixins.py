from abc import ABC

from logsight.connectors.base.connectable import Connectable
from logsight.connectors.base.connector import Connector
from logsight.connectors.base.sink import Sink
from logsight.connectors.base.source import Source


class ConnectableConnector(Connector, Connectable, ABC):
    """Interface that allows the connector to connect to an endpoint"""


class ConnectableSink(Sink, ConnectableConnector, ABC):
    """Interface for Sink that is also able to connect to endpoint."""


class ConnectableSource(Source, ConnectableConnector, ABC):
    """Interface for Source that is also able to connect to endpoint."""
