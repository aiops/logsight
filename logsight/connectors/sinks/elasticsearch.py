import logging
import elasticsearch
from elasticsearch import helpers, Elasticsearch

from .base import Sink

logger = logging.getLogger("logsight." + __name__)


class ElasticsearchSink(Sink):

    def __init__(self, host, port, username, password, private_key=None, application_name=None, index="", **kwargs):
        super().__init__(**kwargs)
        if application_name and private_key:
            self.application_id = "_".join([private_key, application_name])
        else:
            self.application_id = None
        self.index = "_".join([self.application_id, index]) if self.application_id else index
        self.es = Elasticsearch([{'host': host, 'port': port}], http_auth=(username, password))

    def send(self, data):
        if not isinstance(data, list):
            data = [data]
        try:
            print(data)
            helpers.bulk(self.es,
                         data,
                         index=self.index,
                         request_timeout=200)
        except Exception as e:
            logger.error(f"{e}, {data}")
