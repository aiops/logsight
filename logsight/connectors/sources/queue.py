from .base import Source
from time import sleep


class NoQueueException(Exception):
    pass


class SourceQueue(Source):
    def __init__(self, link, **_kwargs):
        super().__init__()
        self.link = link
        self.queue = None

    def connect(self, queue=None):
        if self.queue is None:
            self.queue = queue

    def receive_message(self):
        if self.queue is None:
            raise NoQueueException("Please connect with sink")
        if self.queue.empty():
            sleep(10)
        return self.queue.get()
