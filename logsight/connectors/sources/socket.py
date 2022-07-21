import socket

from connectors import Source
from connectors.connectors.socket.configuration import SocketConfigProperties
from connectors.connectors.socket.connector import SocketConnector


class SocketSource(Source, SocketConnector):

    def __init__(self, config: SocketConfigProperties):
        SocketConnector.__init__(self, config)
        self.max_size = config.max_size

    def _connect(self):
        if self.connected:
            return
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.port))
        self.server_address = self.socket.getsockname()
        self.socket.listen(5)
        self.connected = True

    def receive_message(self) -> str:
        request, client_address = self.socket.accept()
        payload = request.recv(self.max_size).decode("utf-8").strip()
        return payload
