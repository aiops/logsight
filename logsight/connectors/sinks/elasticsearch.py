import logging
from typing import Optional

from elasticsearch import Elasticsearch, helpers
from elasticsearch._sync.client import IngestClient
from tenacity import retry, stop_after_attempt, wait_fixed

from configs.global_vars import RETRY_ATTEMPTS, RETRY_TIMEOUT, ES_PIPELINE_ID_INGEST_TIMESTAMP
from connectors.sinks.sink import ConnectableSink

logger = logging.getLogger("logsight." + __name__)


class ElasticsearchException(Exception):
    pass


class ElasticsearchSink(ConnectableSink):

    def __init__(self, scheme, host, port, username, password, serializer=None):
        super().__init__(serializer)
        self.es = Elasticsearch([{'scheme': scheme, 'host': host, 'port': int(port)}], basic_auth=(username, password))
        self.ingest_client = IngestClient(self.es)

    def close(self):
        self.es.close()

    def _connect(self):
        self.es.ping()
        self._create_timestamp_pipeline()

    def _create_timestamp_pipeline(self):
        resp = self.ingest_client.put_pipeline(
            id=ES_PIPELINE_ID_INGEST_TIMESTAMP,
            description="insert ingest timestamp field to documents",
            processors=[
                {
                    "set": {
                        "field": "ingest_timestamp",
                        "value": "{{_ingest.timestamp}}"
                    }
                }
            ]
        )
        if not resp.body["acknowledged"]:
            raise ElasticsearchException(f"Failed to create ingest timestamp pipeline. Elasticsearch reply: {resp}")

    @retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_fixed(RETRY_TIMEOUT))
    def send(self, data, target: Optional[str] = None):
        if not isinstance(data, list):
            data = [data]
        if not helpers.bulk(
                self.es,
                data,
                pipeline=ES_PIPELINE_ID_INGEST_TIMESTAMP,
                index=target,
                stats_only=True
        ):
            logger.warning(f"Failed to send data to elasticsearch. Retrying...")
            raise ElasticsearchException()

