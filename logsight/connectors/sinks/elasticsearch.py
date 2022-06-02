import logging
from typing import Optional

from elasticsearch import Elasticsearch, helpers
from tenacity import retry, stop_after_attempt, wait_fixed

from configs.global_vars import RETRY_ATTEMPTS, RETRY_TIMEOUT
from connectors.sinks.sink import ConnectableSink

logger = logging.getLogger("logsight." + __name__)


class ElasticsearchSink(ConnectableSink):

    def __init__(self, host, port, username, password, serializer=None):
        super().__init__(serializer)
        self.es = Elasticsearch([{'host': host, 'port': port}], http_auth=(username, password))

    def close(self):
        self.es.close()

    def _connect(self):
        self.es.ping()

    @retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_fixed(RETRY_TIMEOUT))
    def send(self, data, target: Optional[str] = None):
        if not isinstance(data, list):
            data = [data]
        try:
            helpers.parallel_bulk(self.es,
                                  data,
                                  index=target,
                                  request_timeout=200,
                                  thread_count=4, chunk_size=1000)
        except Exception as e:
            logger.warning(f"Failed to send data to elasticsearch. Reason: {e}. Retrying...")
            raise e
