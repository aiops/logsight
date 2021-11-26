import multiprocessing
from collections import deque
from multiprocessing import Queue
from time import sleep

from .base import Sink


class SinkQueue(Sink):
    def __init__(self, link=None, **kwargs):
        super().__init__()
        self.link = link
        self.queue = multiprocessing.Manager().Queue(maxsize=100000)

    def send(self, data):
        if not isinstance(data, list):
            data = [data]
        if self.queue.full():
            print("Full")
            sleep(10)
        for d in data:
            self.queue.put(d)
