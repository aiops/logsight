from logsight.connectors.connectors.elasticsearch import ElasticsearchConfigProperties
from logsight.connectors.connectors.file import FileConfigProperties
from logsight.connectors.connectors.kafka import KafkaConfigProperties
from logsight.connectors.connectors.socket import SocketConfigProperties
from logsight.connectors.connectors.sql_db import DatabaseConfigProperties
from logsight.connectors.connectors.zeromq import ZeroMQConfigProperties


class ConnectorConfigProvider(object):
    configs = {"kafka": KafkaConfigProperties,
               "socket": SocketConfigProperties,
               "file": FileConfigProperties,
               "database": DatabaseConfigProperties,
               "elasticsearch": ElasticsearchConfigProperties,
               "zeromq": ZeroMQConfigProperties}

    def get_config(self, item):
        return self.configs.get(item, None)
