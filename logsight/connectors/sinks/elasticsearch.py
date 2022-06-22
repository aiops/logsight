from typing import Optional

from elasticsearch import Elasticsearch
from tenacity import retry, stop_after_attempt, wait_fixed

from configs.global_vars import RETRY_ATTEMPTS, RETRY_TIMEOUT
from connectors.base.mixins import ConnectableSink
from connectors.connectors.elasticsearch import ElasticsearchConnector


class ElasticsearchSink(ConnectableSink, ElasticsearchConnector):

    def __init__(self, scheme, host, port, username, password):
        ElasticsearchConnector.__init__(self, scheme, host, port, username, password, ingest_timestamp=True)
        self.es = Elasticsearch([{'scheme': scheme, 'host': host, 'port': int(port)}], basic_auth=(username, password))

    @retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_fixed(RETRY_TIMEOUT))
    def send(self, data, target: Optional[str] = None):
        return self.parallel_bulk(data, target, pipeline=True)
