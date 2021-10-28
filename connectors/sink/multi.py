from typing import List

from .base import Sink


class MultiSink(Sink):
    def __init__(self, sinks: List[Sink], **kwargs):
        super().__init__(**kwargs)
        self.sinks = sinks

    def send(self, data):
        for sink in self.sinks:
            sink.send(data)
