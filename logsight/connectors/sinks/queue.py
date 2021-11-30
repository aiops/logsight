from collections import deque

from .base import Sink


class SinkQueue(Sink):
    def __init__(self, link=None, **kwargs):
        super().__init__(**kwargs)
        self.link = link
        self.queue = deque(maxlen=6000)

    def send(self, data):
        if not isinstance(data, list):
            data = [data]
        for d in data:
            self.queue.appendleft(d)
