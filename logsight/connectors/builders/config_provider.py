from connectors.connectors.kafka import KafkaConfigProperties
from connectors.connectors.file import FileConfigProperties
from connectors.connectors.zeromq import ZeroMQConfigProperties
from connectors.connectors.sql_db import DatabaseConfigProperties
from connectors.connectors.socket import SocketConfigProperties
from connectors.connectors.elasticsearch import ElasticsearchConfigProperties


class ConnectorConfigProvider(object):
    configs = {"kafka": KafkaConfigProperties,
               "socket": SocketConfigProperties,
               "file": FileConfigProperties,
               "database": DatabaseConfigProperties,
               "elasticsearch": ElasticsearchConfigProperties,
               "zeromq": ZeroMQConfigProperties}

    def get_config(self, item):
        return self.configs.get(item, None)
