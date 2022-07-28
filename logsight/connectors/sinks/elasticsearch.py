from typing import Optional

from tenacity import retry, stop_after_attempt, wait_fixed

from logsight.configs.properties import LogsightProperties
from logsight.connectors.base.mixins import ConnectableSink
from logsight.connectors.connectors.elasticsearch import ElasticsearchConfigProperties, ElasticsearchConnector

config_properties = LogsightProperties()


class ElasticsearchSink(ConnectableSink, ElasticsearchConnector):

    def __init__(self, config: ElasticsearchConfigProperties):
        ElasticsearchConnector.__init__(self, config)

    @retry(reraise=True,
           stop=stop_after_attempt(config_properties.retry_attempts),
           wait=wait_fixed(config_properties.retry_timeout))
    def send(self, data, target: Optional[str] = None):
        return self.parallel_bulk(data, target, pipeline=True)
