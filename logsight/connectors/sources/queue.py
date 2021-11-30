import json
from collections import deque
from .base import Source
import multiprocessing


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
        # if not isinstance(self.queue, deque):
        #     raise NoQueueException("Please connect with sink")
        if self.queue.empty():
            sleep(2)
        return self.queue.pop()
