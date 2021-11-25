from .base import Sink


class EmptySink(Sink):
    def send(self, data):
        pass
