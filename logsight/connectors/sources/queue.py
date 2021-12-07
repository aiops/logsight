import json
from collections import deque
from .base import Source
import multiprocessing
from time import sleep

class NoQueueException(Exception):
    pass


class SourceQueue(Source):
    def __init__(self, link, **kwargs):
        super().__init__()
        self.link = link
        self.queue = None

    def connect(self, queue=None):
        if self.queue is None:
            self.queue = queue

    def receive_message(self):
        if self.queue == None:
            raise Exception("Please connect with sink")
        if self.queue.empty():
            print("Empty")
            sleep(10)
        return self.queue.get()
