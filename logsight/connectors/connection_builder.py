from copy import deepcopy
from typing import Optional, Type, Union

import connectors
from common.logsight_classes.configs import ConnectionConfig
from common.patterns.builder import Builder
from connectors import Connector, Source, serializers
from connectors.sources.source import ConnectableSource
from services import ConnectionConfigParser


class ConnectionBuilder(Builder):
    def __init__(self, config: Optional[ConnectionConfigParser] = None):
        self.conn_config = config if config else ConnectionConfigParser()

    def build(self, config: ConnectionConfig) -> Union[Source, Type[Connector], Type[Source], ConnectableSource]:
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
