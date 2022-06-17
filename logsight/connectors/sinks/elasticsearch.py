import logging
from typing import Optional

from elasticsearch import Elasticsearch, helpers
from elasticsearch.client import IngestClient
from tenacity import retry, stop_after_attempt, wait_fixed

from configs.global_vars import RETRY_ATTEMPTS, RETRY_TIMEOUT, ES_PIPELINE_ID_INGEST_TIMESTAMP
from connectors.base.elasticsearch import ElasticsearchConnector
from connectors.sinks.sink import ConnectableSink

logger = logging.getLogger("logsight." + __name__)


class ElasticsearchSink(ConnectableSink, ElasticsearchConnector):

    def __init__(self, scheme, host, port, username, password, serializer=None):
        super().__init__(serializer)
        self.es = Elasticsearch([{'scheme': scheme, 'host': host, 'port': int(port)}], basic_auth=(username, password))

    def _connect(self):
        self.es.ping()

    @retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_fixed(RETRY_TIMEOUT))
    def send(self, data, target: Optional[str] = None):
        return self.parallel_bulk(data, target, pipeline=True)
