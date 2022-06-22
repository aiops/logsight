from copy import deepcopy
from typing import Optional, Type

from common.logsight_classes.configs import AdapterConfigProperties
from common.patterns.builder import Builder
from connectors.base import Adapter, Source
from connectors.base.connectable import Connectable
from connectors.base.mixins import ConnectableSinkAdapter, ConnectableSourceAdapter, SinkAdapter, SourceAdapter
from connectors.builders.cls_dict import cls_conn
from connectors.serializers import DefaultSerializer
from services.configurator.config_manager import ConnectionConfig
from connectors import serializers


class AdapterBuilder(Builder):
    def __init__(self, config: Optional[ConnectionConfig] = None):
        self.conn_config = config if config else ConnectionConfig()

    def build(self, config: AdapterConfigProperties) -> Adapter:
        """
          It takes a connection configuration and returns a Connector object.
          Args:
              config: (ConnectionConfig): Connector configuration object

          Returns:
            Type[ConnectableConnector]: A `Connector` object
      """
        conn_params = deepcopy(self.conn_config.get_connection(config.connector.connection))
        conn_params.update(config.connector.params)
        c_name = cls_conn[config.connector.connector_type][config.connector.connection]
        serializer = getattr(serializers, config.serializer)() if config.serializer else DefaultSerializer()
        connector = c_name(**conn_params)
        adapter_cls = self._get_adapter(connector)
        return adapter_cls(connector, serializer)

    @staticmethod
    def _get_adapter(connector):
        if isinstance(connector, Source):
            if isinstance(connector, Connectable):
                return ConnectableSourceAdapter
            return SourceAdapter
        else:
            if isinstance(connector, Connectable):
                return ConnectableSinkAdapter
            return SinkAdapter
