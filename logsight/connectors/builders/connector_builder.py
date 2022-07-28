from typing import Type, Union

from logsight.common.patterns.builder import Builder
from logsight.connectors.builders.cls_dict import cls_conn
from logsight.connectors.builders.config_provider import ConnectorConfigProvider
from logsight.connectors.builders.properties import ConnectorConfigProperties
from logsight.connectors import Connectable, Sink, Source


class ConnectorBuilder(Builder):
    def __init__(self):
        self.conn_config = ConnectorConfigProvider()

    def build(self, config: ConnectorConfigProperties) -> Union[Connectable, Source, Sink]:
        """
          It takes a connection configuration and returns a Connector object.
          Args:
              config: (ConnectionConfig): Connector configuration object

          Returns:
            Type[ConnectableConnector]: A `Connector` object
      """
        c_name = cls_conn[config.connector_type][config.connection]
        config_cls = self.conn_config.get_config(config.connection)
        if config_cls:
            conn_config = config_cls(**config.params)
            return c_name(conn_config)
        return c_name()
