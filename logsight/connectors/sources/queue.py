import json
from collections import deque
from .base import Source


class NoQueueException(Exception):
    pass


class SourceQueue(Source):
    def __init__(self, link, **kwargs):
        super().__init__(**kwargs)
        self.link = link
        self.queue = None

    def connect(self, queue=None):
        if self.queue is None:
            self.queue = queue

    def receive_message(self):
        if not isinstance(self.queue, deque):
            raise NoQueueException("Please connect with sink")
        if not self.queue:
            return
        return self.queue.pop()
