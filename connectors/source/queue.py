import json

from .base import Source


class SourceQueue(Source):
    def __init__(self, link, **kwargs):
        super().__init__()
        self.link = link
        self.queue = None

    def connect(self, queue):
        self.queue = queue

    def receive_message(self):
        if not self.queue:
            raise Exception("Please connect with sink")
        if self.queue.empty():
            return
        return json.loads(self.queue.get(0))

    def process_message(self):
        pass
