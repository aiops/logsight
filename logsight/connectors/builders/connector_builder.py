from copy import deepcopy
from typing import Optional, Type

from common.logsight_classes.configs import ConnectorConfigProperties
from common.patterns.builder import Builder
from connectors.base import Adapter
from connectors.builders.cls_dict import cls_conn
from services.configurator.config_manager import ConnectionConfig


class ConnectorBuilder(Builder):
    def __init__(self, config: Optional[ConnectionConfig] = None):
        self.conn_config = config if config else ConnectionConfig()

    def build(self, config: ConnectorConfigProperties) -> Adapter:
        """
          It takes a connection configuration and returns a Connector object.
          Args:
              config: (ConnectionConfig): Connector configuration object

          Returns:
            Type[ConnectableConnector]: A `Connector` object
      """
        conn_params = deepcopy(self.conn_config.get_connection(config.connection))
        conn_params.update(config.params)
        c_name = cls_conn[config.connector_type][config.connection]
        return c_name(**conn_params)
