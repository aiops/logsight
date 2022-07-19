import json
import socket

from connectors.base.mixins import ConnectableSource


class SocketSource(ConnectableSource):

    def __init__(self, host: str, port, max_size: int = 2048):
        self.server_address = None
        self.host = host
        self.port = port
        self.max_size = max_size
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._data = None
        self.connected = False

    def close(self):
        self.socket.close()
        self.connected = False

    def _connect(self):
        if self.connected:
            return
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.port))
        self.server_address = self.socket.getsockname()
        self.socket.listen(5)
        self.connected = True

    def receive_message(self):
        request, client_address = self.socket.accept()
        payload = request.recv(self.max_size).decode("utf-8").strip()
        return json.loads(payload)
