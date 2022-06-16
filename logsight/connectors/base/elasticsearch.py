import logging

from elasticsearch import Elasticsearch
from connectors import Connector

logger = logging.getLogger("logsight." + __name__)


class ElasticsearchConnector(Connector):
    def __init__(self, scheme, host, port, username, password, **_kwargs):
        self.es = Elasticsearch([{'scheme': scheme, 'host': host, 'port': int(port)}], basic_auth=(username, password))
        self.host = host
        self.port = port

    def _connect(self):
        if not self.es.ping():
            msg = f"Elasticsearch endpoint {self.host}:{self.port} is unreachable."
            logger.error(msg)
            raise ConnectionError(msg)

    def close(self):
        self.es.close()
