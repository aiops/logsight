from typing import List

from .sink import Sink


class MultiSink(Sink):
    def close(self):
        for sink in self.sinks:
            sink.close()

    def __init__(self, sinks: List[Sink], **kwargs):
        super().__init__(**kwargs)
        self.sinks = sinks

    def send(self, data):
        for sink in self.sinks:
            sink.send(data)

    def connect(self):
        for sink in self.sinks:
            sink.connect()
