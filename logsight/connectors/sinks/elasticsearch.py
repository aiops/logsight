import logging

from elasticsearch import Elasticsearch, helpers
from tenacity import retry, stop_after_attempt, wait_fixed

from .sink import Sink

logger = logging.getLogger("logsight." + __name__)


class ElasticsearchSink(Sink):

    def __init__(self, host, port, username, password, private_key=None, application_key=None, index="", **kwargs):
        if application_key and private_key:
            self.application_id = "_".join([private_key, application_key])
        else:
            self.application_id = None
        self.index = "_".join([self.application_id, index]) if self.application_id else index
        self.es = Elasticsearch([{'host': host, 'port': port}], http_auth=(username, password))

    def close(self):
        self.es.close()

    def connect(self):
        pass

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(5))
    def send(self, data):
        if not isinstance(data, list):
            data = [data]
        try:
            helpers.bulk(self.es,
                         data,
                         index=self.index,
                         request_timeout=200)
        except Exception as e:
            logger.warning(f"Failed to send data to elasticsearch. Reason: {e}. Retrying...")
            raise e
