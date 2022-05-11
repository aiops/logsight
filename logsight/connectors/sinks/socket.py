import json
import socket

from .sink import ConnectableSink


class SocketSink(ConnectableSink):
    def __init__(self, host, port, serializer=None):
        super().__init__(serializer)
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def send(self, data, index=None):
        if not isinstance(data, list):
            data = [data]
        for d in data:
            self.socket.sendall(bytes(json.dumps(d, default=list) + "\n", "utf-8"))

    def connect(self):
        try:
            self.socket.connect((self.host, self.port))
        except Exception as e:
            print(self.__class__.__name__, self.host, self.port, e)

    def close(self):
        self.socket.close()
