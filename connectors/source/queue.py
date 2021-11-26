import json
from collections import deque
from multiprocessing import Queue
from time import sleep

from .base import Source


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
            sleep(2)
        return self.queue.get()

    def process_message(self):
        pass
