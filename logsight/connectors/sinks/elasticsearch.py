import logging
from typing import Optional

from elasticsearch import Elasticsearch, helpers
from tenacity import retry, stop_after_attempt, wait_fixed

from connectors.sinks.sink import ConnectableSink

logger = logging.getLogger("logsight." + __name__)


class ElasticsearchSink(ConnectableSink):

    def __init__(self, host, port, username, password):
        super().__init__()
        self.es = Elasticsearch([{'host': host, 'port': port}], http_auth=(username, password))

    def close(self):
        self.es.close()

    def connect(self):
        self.es.ping()

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(5))
    def send(self, data, index: Optional[str] = None):
        if not isinstance(data, list):
            data = [data]
        try:
            helpers.bulk(self.es,
                         data,
                         index=index,
                         request_timeout=200)
        except Exception as e:
            logger.warning(f"Failed to send data to elasticsearch. Reason: {e}. Retrying...")
            raise e
