import json
import socket
from socketserver import BaseServer, BaseRequestHandler
from .base import Source


class SocketSource(Source):
    def __init__(self, host, port, **kwargs):
        super().__init__(**kwargs)
        self.server_address = None
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._data = None
        self.connected = False

    def connect(self):
        if self.connected:
            return
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.port))
        self.server_address = self.socket.getsockname()
        self.socket.listen(5)
        self.connected = True

    def receive_message(self):
        request, client_address = self.socket.accept()
        return json.loads(request.recv(2048).decode("utf-8").strip())

