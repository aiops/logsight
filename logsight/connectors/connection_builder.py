from copy import deepcopy
from typing import Optional, Type, Union

import connectors
from common.logsight_classes.configs import ConnectionConfigProperties
from common.patterns.builder import Builder
from connectors import Connector, Source, serializers
from connectors.sources.source import ConnectableSource, LogBatchConnectableSource, LogBatchSource
from services.configurator.config_manager import ConnectionConfig


class ConnectionBuilder(Builder):
    def __init__(self, config: Optional[ConnectionConfig] = None):
        self.conn_config = config if config else ConnectionConfig()

    def build(self, config: ConnectionConfigProperties) -> Union[Type[Connector], Type[Source], ConnectableSource]:
        """
          It takes a connection configuration and returns a Connector object.
          Args:
              config: (ConnectionConfig): Connector configuration object

          Returns:
            Type[Connector]: A `Connector` object
      """
        conn_params = deepcopy(self.conn_config.get_connection(config.connection))
        conn_params.update(config.params)
        c_name = getattr(connectors, config.classname)
        serializer = getattr(serializers, config.serializer)() if config.serializer else None
        return c_name(**conn_params, serializer=serializer)


class PipelineConnectionBuilder(Builder):
    def __init__(self, config: Optional[ConnectionConfig] = None):
        self.conn_config = config if config else ConnectionConfig()

    def build(self, config: ConnectionConfigProperties) -> Union[LogBatchConnectableSource, LogBatchSource]:
        """
          It takes a connection configuration and returns a Connector object.
          Args:
              config: (ConnectionConfig): Connector configuration object

          Returns:
            Type[Connector]: A `Connector` object
      """
        conn_params = deepcopy(self.conn_config.get_connection(config.connection))
        conn_params.update(config.params)
        c_name = getattr(connectors, config.classname)
        if ConnectableSource in c_name.__bases__:
            c_name.__bases__ = modify_base(LogBatchConnectableSource, ConnectableSource, c_name.__bases__)
        elif Source in c_name.__bases__:
            c_name.__bases__ = modify_base(LogBatchSource, Source, c_name.__bases__)
        serializer = getattr(serializers, config.serializer)() if config.serializer else None
        return c_name(**conn_params, serializer=serializer)


def modify_base(target, base, bases):
    return tuple([target if x is base else x for x in bases])
