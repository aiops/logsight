import multiprocessing
from .base import Sink
from time import sleep


class SinkQueue(Sink):
    def __init__(self, link=None, **_kwargs):
        super().__init__()
        self.link = link
        self.queue = multiprocessing.Manager().Queue(maxsize=100000)

    def send(self, data):
        if not isinstance(data, list):
            data = [data]
        if self.queue.full():
            print("Full")
            sleep(2)
        for d in data:
            self.queue.put(d)
