import threading

from elasticsearch.client import Elasticsearch

from .base import Source
from modules.core.wrappers import synchronized


class ElasticsearchListenerSource(Source):

    def __init__(self, host, port, username, password, query, pull_interval, **kwargs):
        super().__init__(**kwargs)

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

    @synchronized
    def _timeout_call(self):
        self.buffer.append(self.es.scan(self.query))
        self.timer.cancel()
        self.timer = threading.Timer(self.pull_interval, self._timeout_call)
        self.timer.start()
