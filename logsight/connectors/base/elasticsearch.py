import logging

from elasticsearch import Elasticsearch
from connectors import Connector

logger = logging.getLogger("logsight." + __name__)


class ElasticsearchConnector(Connector):
    def __init__(self, host, port, username, password, **_kwargs):
        self.es = Elasticsearch([{'host': host, 'port': port}], http_auth=(username, password))
        self.host = host
        self.port = port

    def _connect(self):
        logger.debug(f"Verifying elasticsearch connection on {self.host}:{self.port}.")
        if not self.es.ping():
            msg = f"Elasticsearch endpoint {self.host}:{self.port} is unreachable."
            logger.error(msg)
            raise ConnectionError(msg)
        logger.info("Elasticsearch connected.")

    def close(self):
        logger.info(f"Closing elasticsearch connection.")
        self.es.close()
