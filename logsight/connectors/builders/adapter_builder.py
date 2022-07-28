from typing import Type

from logsight.common.patterns.builder import Builder
from logsight.connectors import Sink, serializers
from logsight.connectors.base import Adapter, Source
from logsight.connectors.base.adapter import SinkAdapter, SourceAdapter
from logsight.connectors.builders.connector_builder import ConnectorBuilder
from logsight.connectors.builders.properties import AdapterConfigProperties
from logsight.connectors.serializers import DefaultSerializer


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
