import json
import socket

from .sink import Sink


class SocketSink(Sink):
    def connect(self):
        try:
            self.socket.connect((self.host, self.port))
        except Exception as e:
            print(self.__class__.__name__, self.host, self.port, e)

    def close(self):
        self.socket.close()

    def __init__(self, host, port, **kwargs):
        super().__init__()
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def send(self, data):
        if not isinstance(data, list):
            data = [data]
        for d in data:
            self.socket.sendall(bytes(json.dumps(d) + "\n", "utf-8"))
