import threading

from elasticsearch.client import Elasticsearch

from .base import Source, StreamSource
from modules.api.wrappers import synchronized


class ElasticsearchStreamSource(StreamSource):
    def __init__(self, host, port, username, password, query, pull_interval, **kwargs):
        super().__init__()

        self.es = Elasticsearch([{'host': host, 'port': port}],
                                http_auth=(username, password))

        self.query = query
        self.pull_interval = pull_interval
        self.timer = threading.Timer(self.pull_interval, self._timeout_call)
        self.buffer = []

    def connect(self):
        self.timer.start()

    @synchronized
    def receive_message(self):
        result = self.buffer.copy()
        self.buffer = []  # reset buffer
        return result

    def process_message(self):
        pass

    @synchronized
    def _timeout_call(self):
        self.buffer.append(self.es.scan(self.query))
        self.timer.cancel()
        self.timer = threading.Timer(self.pull_interval, self._timeout_call)
        self.timer.start()


class ElasticsearchSource(Source):
    def __init__(self, host, port, username, password, **kwargs):
        super().__init__()

        self.es = Elasticsearch([{'host': host, 'port': port}],
                                http_auth=(username, password))

    def connect(self):
        pass

    def get_data(self, query):
        return self.es.scan(query)
