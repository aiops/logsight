from abc import ABC
from typing import Any, Optional

from connectors.base.adapter import Adapter
from connectors.base.connectable import Connectable
from connectors.base.connector import Connector
from connectors.base.serializer import Serializer
from connectors.base.sink import Sink
from connectors.base.source import Source


class ConnectableConnector(Connector, Connectable, ABC):
    """Interface that allows the connector to connect to an endpoint"""


class ConnectableSink(Sink, ConnectableConnector, ABC):
    """Interface for Sink that is also able to connect to endpoint."""


class ConnectableSource(Source, ConnectableConnector, ABC):
    """Interface for Source that is also able to connect to endpoint."""


class SourceAdapter(Adapter, Source):
    def __init__(self, connector: Source, serializer: Serializer):
        super().__init__(serializer=serializer)
        self.connector = connector

    def receive_message(self) -> Any:
        return self.serializer.deserialize(self.connector.receive_message())


class SinkAdapter(Adapter, Sink):

    def __init__(self, connector: Sink, serializer: Serializer):
        super().__init__(serializer=serializer)
        self.connector = connector

    def send(self, data: Any, target: Optional[Any] = None):
        self.connector.send(self.serializer.serialize(data))


class ConnectableSourceAdapter(SourceAdapter, ConnectableConnector):
    def __init__(self, connector: ConnectableSource, serializer: Serializer):
        super(SourceAdapter, self).__init__(serializer=serializer)
        self.connector = connector

    def _connect(self):
        self.connector._connect()

    def close(self):
        self.connector.close()


class ConnectableSinkAdapter(SinkAdapter, ConnectableConnector):

    def __init__(self, connector: ConnectableSink, serializer: Serializer):
        super(SinkAdapter, self).__init__(serializer=serializer)
        self.connector = connector

    def _connect(self):
        self.connector._connect()

    def close(self):
        self.connector.close()
