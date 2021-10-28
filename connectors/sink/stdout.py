from .base import Sink


class PrintSink(Sink):
    def send(self, data):
        print(f"[SINK] Sending data: {data}")
