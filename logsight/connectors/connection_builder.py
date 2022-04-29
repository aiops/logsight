from copy import deepcopy
from typing import Optional, Type, Union

import connectors
from connectors import Connector, Source, transformers
from connectors.sources.source import ConnectionSource
from common.logsight_classes.configs import ConnectionConfig
from common.patterns.builder import Builder
from services import ConnectionConfigParser


class ConnectionBuilder(Builder):
    def __init__(self, config: Optional[ConnectionConfigParser] = None):
        self.conn_config = config if config else ConnectionConfigParser()

    def build(self, config: ConnectionConfig) -> Union[Source, Type[Connector], Type[Source], ConnectionSource]:
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
        transformer = getattr(transformers, config.transformer)() if config.transformer else None
        return c_name(**conn_params, transformer=transformer)
