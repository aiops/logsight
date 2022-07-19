from abc import ABC
from typing import Any, Optional, Union

from connectors.base.connectable import Connectable
from connectors.base.serializer import Serializer
from connectors.base.source import HasNextMixin, Source
from connectors.base.sink import Sink


class Adapter(ABC):
    """Base interface containing connector and serializer interfaces"""

    def __init__(self, connector: Union[Connectable, Source, Sink] = None, serializer: Serializer = None):
        self.connector = connector
        self.serializer = serializer


class SourceAdapter(Adapter, HasNextMixin):
    def receive(self) -> Any:
        return self.serializer.deserialize(self.connector.receive_message())


class SinkAdapter(Adapter, HasNextMixin):
    def send(self, data: Any, target: Optional[Any] = None):
        self.connector.send(self.serializer.serialize(data), target)


class AdapterError(Exception):
    """General exception thrown by adapters"""
