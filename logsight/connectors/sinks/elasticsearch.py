from typing import Optional

from tenacity import retry, stop_after_attempt, wait_fixed

from connectors.base.mixins import ConnectableSink
from connectors.connectors.elasticsearch import ElasticsearchConnector, ElasticsearchConfigProperties

from configs.properties import LogsightProperties

config_properties = LogsightProperties()


class ElasticsearchSink(ConnectableSink, ElasticsearchConnector):

    def __init__(self, config: ElasticsearchConfigProperties):
        ElasticsearchConnector.__init__(self, config)

    @retry(reraise=True,
           stop=stop_after_attempt(config_properties.retry_attempts),
           wait=wait_fixed(config_properties.retry_timeout))
    def send(self, data, target: Optional[str] = None):
        return self.parallel_bulk(data, target, pipeline=True)
