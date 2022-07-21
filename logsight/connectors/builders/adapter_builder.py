from copy import deepcopy
from typing import Optional, Type

from common.patterns.builder import Builder
from connectors.base import Adapter, Source
from connectors.base.adapter import SinkAdapter, SourceAdapter
from connectors.builders.connector_builder import ConnectorBuilder
from connectors.builders.properties import AdapterConfigProperties
from connectors.serializers import DefaultSerializer
from connectors import Sink, serializers


class AdapterBuilder(Builder):
    def __init__(self):
        self.connector_builder = ConnectorBuilder()

    def build(self, config: AdapterConfigProperties) -> Adapter:
        """
          It takes a connection configuration and returns a Connector object.
          Args:
              config: (ConnectionConfig): Connector configuration object

          Returns:
            Type[ConnectableConnector]: A `Connector` object
      """
        serializer = getattr(serializers, config.serializer)() if config.serializer else DefaultSerializer()
        connector = self.connector_builder.build(config.connector)
        adapter_cls = self._get_adapter(connector)
        return adapter_cls(connector, serializer)

    @staticmethod
    def _get_adapter(connector):
        if isinstance(connector, Source):
            return SourceAdapter
        elif isinstance(connector, Sink):
            return SinkAdapter
        else:
            return Adapter
