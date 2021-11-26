import json
from collections import deque
from multiprocessing import Queue

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
        # if not self.queue:
        #     return
        return self.queue.get()

    def process_message(self):
        pass
