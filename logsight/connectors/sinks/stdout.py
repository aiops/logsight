from .sink import Sink


class PrintSink(Sink):
    def connect(self):
        pass

    def close(self):
        pass

    def send(self, data):
        print(f"[SINK] Sending data: {data}")
