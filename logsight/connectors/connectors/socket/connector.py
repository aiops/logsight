import socket

from logsight.connectors import ConnectableConnector
from logsight.connectors.connectors.socket.configuration import SocketConfigProperties


class SocketConnector(ConnectableConnector):
    def __init__(self, config: SocketConfigProperties):
        self.host = config.host
        self.port = config.port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connected = False

    def _connect(self):
        raise NotImplementedError

    def close(self):
        self.socket.close()
        self.connected = False
