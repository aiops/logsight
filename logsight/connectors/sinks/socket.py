import json
import logging
from typing import Any, Optional

from connectors import Sink
from connectors.connectors.socket.configuration import SocketConfigProperties
from connectors.connectors.socket.connector import SocketConnector

logger = logging.getLogger("logsight." + __name__)


class SocketSink(Sink, SocketConnector):

    def __init__(self, config: SocketConfigProperties):
        SocketConnector.__init__(self, config)

    def send(self, data: Any, target: Optional[Any] = None):
        if not isinstance(data, list):
            data = [data]
        for d in data:
            self.socket.sendall(bytes(json.dumps(d, default=list) + "\n", "utf-8"))

    def _connect(self):
        if self.connected:
            return
        try:
            self.socket.connect((self.host, self.port))
            self.connected = True
        except Exception as e:
            logger.error(f"Unable to connect to socket on {self.host:self.port}. Reason {e}")
            raise e
