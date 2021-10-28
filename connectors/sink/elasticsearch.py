from elasticsearch import helpers, Elasticsearch

from .base import Sink


class ElasticsearchSink(Sink):

    def __init__(self, host, port, username, password, application_id="", index="", **kwargs):
        super().__init__()
        self.index = "_".join([application_id, index])
        self.es = Elasticsearch([{'host': host, 'port': port}], http_auth=(username, password))

    def send(self, data):
        if not isinstance(data, list):
            data = [data]
        helpers.bulk(self.es,
                     data,
                     index=self.index,
                     doc_type='_doc',
                     request_timeout=200)
