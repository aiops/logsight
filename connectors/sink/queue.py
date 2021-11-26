from collections import deque

from .base import Sink


class SinkQueue(Sink):
    def __init__(self, link=None, **kwargs):
        super().__init__()
        self.link = link
        self.queue = deque(maxlen=10000)

    def send(self, data):
        if not isinstance(data, list):
            data = [data]
        for d in data:
            self.queue.append(d)
