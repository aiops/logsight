import logging

from elasticsearch import Elasticsearch, NotFoundError, helpers
from elasticsearch.client import IngestClient

from logsight.connectors.base.mixins import ConnectableConnector
from .configuration import ElasticsearchConfigProperties

logger = logging.getLogger("logsight." + __name__)


class ElasticsearchException(Exception):
    pass


class ElasticsearchConnector(ConnectableConnector):
    def __init__(self, config: ElasticsearchConfigProperties):
        self.es = Elasticsearch([{'scheme': config.scheme, 'host': config.host, 'port': config.port}],
                                basic_auth=(config.username, config.password))
        self.ingest_pipeline = config.ingest_pipeline
        self.host = config.host
        self.port = config.port
        self.max_fetch_size = config.max_fetch_size
        
    def _connect(self):
        self._verify_connection()
        self._create_timestamp_pipeline()

    def _verify_connection(self):
        if not self.es.ping(human=True):
            msg = f"Elasticsearch endpoint {self.host}:{self.port} is unreachable."
            logger.error(msg)
            raise ConnectionError(msg)

    def _create_timestamp_pipeline(self):
        if not self.ingest_pipeline:
            return
        client = IngestClient(self.es)
        try:
            client.get_pipeline(id=self.ingest_pipeline, summary=True)
        except NotFoundError:
            resp = client.put_pipeline(
                id=self.ingest_pipeline,
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
                raise ElasticsearchException(f"Failed to create ingest timestamp pipeline. "
                                             f"Elasticsearch reply: {resp}")

    def bulk(self, data, index: str, pipeline: bool = False):
        ingest_pipeline = self.ingest_pipeline if pipeline else None
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
        ingest_pipeline = self.ingest_pipeline if pipeline else None
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
