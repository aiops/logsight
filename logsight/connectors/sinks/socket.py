import socket
import json
from .sink import Sink


class SocketSink(Sink):
    def __init__(self, host, port, **kwargs):
        super().__init__(**kwargs)
        self.host = host
        self.port = port

    def send(self, data):
        if not isinstance(data, list):
            data = [data]

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Connect to server and send data
            try:
                sock.connect((self.host, self.port))
            except Exception as e:
                print(self.__class__.__name__, self.host, self.port, e)
            for d in data:
                sock.sendall(bytes(json.dumps(d) + "\n", "utf-8"))
