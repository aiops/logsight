from time import time

from .base import Sink


class EmptySink(Sink):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cnt = 0
        self.time = time()

    def send(self, data):
        self.cnt += 1
        if self.cnt % 1000 == 0:
            print("Processed", self.cnt, time() - self.time)

