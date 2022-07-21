from typing import Optional

from tenacity import retry, stop_after_attempt, wait_fixed

from configs.global_vars import RETRY_ATTEMPTS, RETRY_TIMEOUT
from connectors.base.mixins import ConnectableSink
from connectors.connectors.elasticsearch import ElasticsearchConnector, ElasticsearchConfigProperties


class ElasticsearchSink(ConnectableSink, ElasticsearchConnector):

    def __init__(self, config: ElasticsearchConfigProperties):
        ElasticsearchConnector.__init__(self, config)

    @retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_fixed(RETRY_TIMEOUT))
    def send(self, data, target: Optional[str] = None):
        return self.parallel_bulk(data, target, pipeline=True)
