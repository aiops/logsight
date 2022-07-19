import json
import logging
import socket
from typing import Any, Optional

from connectors.base.mixins import ConnectableSink

logger = logging.getLogger("logsight." + __name__)


class SocketSink(ConnectableSink):

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def send(self, data: Any, target: Optional[Any] = None):
        if not isinstance(data, list):
            data = [data]
        for d in data:
            self.socket.sendall(bytes(json.dumps(d, default=list) + "\n", "utf-8"))

    def _connect(self):
        try:
            self.socket.connect((self.host, self.port))
        except Exception as e:
            logger.error(f"Unable to connect to socket on {self.host:self.port}. Reason {e}")
            raise e

    def close(self):
        self.socket.close()
