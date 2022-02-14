from .sink import Sink


class PrintSink(Sink):
    def send(self, data):
        print(f"[SINK] Sending data: {data}")
