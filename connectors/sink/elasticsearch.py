from elasticsearch import helpers, Elasticsearch

from .base import Sink


class ElasticsearchSink(Sink):

    def __init__(self, host, port, username, password, private_key=None, application_name=None, index="", **kwargs):
        super().__init__()
        if application_name and private_key:
            self.application_id = "_".join([private_key, application_name])
        else:
            self.application_id = None
        self.index = "_".join([self.application_id, index]) if self.application_id else index
        self.es = Elasticsearch([{'host': host, 'port': port}], http_auth=(username, password))

    def send(self, data):
        if not isinstance(data, list):
            data = [data]
        helpers.bulk(self.es,
                     data,
                     index=self.index,
                     request_timeout=200)
