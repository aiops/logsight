import logging

from elasticsearch import Elasticsearch, NotFoundError, helpers
from elasticsearch.client import IngestClient

from configs.global_vars import ES_PIPELINE_ID_INGEST_TIMESTAMP
from connectors import Connector

logger = logging.getLogger("logsight." + __name__)


class ElasticsearchException(Exception):
    pass


class ElasticsearchConnector(Connector):
    def __init__(self, scheme, host, port, username, password, **kwargs):
        self.es = Elasticsearch([{'scheme': scheme, 'host': host, 'port': int(port)}], basic_auth=(username, password))
        self.ingest_client = IngestClient(self.es)
        self.host = host
        self.port = port

    def _connect(self):
        if not self.es.ping():
            msg = f"Elasticsearch endpoint {self.host}:{self.port} is unreachable."
            logger.error(msg)
            raise ConnectionError(msg)
        self._create_timestamp_pipeline()

    def _create_timestamp_pipeline(self):
        try:
            self.ingest_client.get_pipeline(id=ES_PIPELINE_ID_INGEST_TIMESTAMP, summary=True)
        except NotFoundError:
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

    def bulk(self, data, index: str, pipeline: bool = False):
        ingest_pipeline = ES_PIPELINE_ID_INGEST_TIMESTAMP if pipeline else None
        if not isinstance(data, list):
            data = [data]
        try:
            return helpers.bulk(self.es,
                                data,
                                index=index,
                                pipeline=ingest_pipeline,
                                request_timeout=200)
        except Exception as e:
            logger.warning(f"Failed to send data to elasticsearch. Reason: {e}. Retrying...")
            raise e

    def parallel_bulk(self, data, index: str, pipeline: bool = False):
        ingest_pipeline = ES_PIPELINE_ID_INGEST_TIMESTAMP if pipeline else None
        if not isinstance(data, list):
            data = [data]
        for success, info in helpers.parallel_bulk(self.es, self.insert_data(data, index),
                                                   pipeline=ingest_pipeline,
                                                   thread_count=8,
                                                   request_timeout=200):
            if not success:
                logger.info(f"Failed to send data to elasticsearch. Retrying...")
                raise ElasticsearchException()
            return info

    @staticmethod
    def insert_data(documents, index):
        for document in documents:
            yield {
                '_op_type': 'index',
                '_index': index,
                '_source': document
            }

    def close(self):
        self.es.close()
