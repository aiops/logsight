import logging
import time
from enum import Enum
from typing import Optional

import zmq
from zmq import Socket

from connectors.sinks import Sink
from connectors.sources import Source

logger = logging.getLogger("logsight." + __name__)


class ConnectionTypes(Enum):
    BIND = 1
    CONNECT = 2


class ZeroMQBase(Source, Sink):
    name = "zeromq"

    def __init__(self, endpoint: str, socket_type: zmq.constants,
                 connection_type: ConnectionTypes = ConnectionTypes.BIND, num_connect_retry: int = 5):
        super().__init__()
        self.endpoint = endpoint
        self.socket_type = socket_type
        self.num_connect_retry = num_connect_retry
        self.socket: Optional[Socket] = None
        self.connection_type = connection_type

    def connect(self):
        attempt = 0
        while attempt < self.num_connect_retry:
            attempt += 1
            logger.info(f"Setting up ZeroMQ socket on {self.endpoint}.")
            context = zmq.Context()
            self.socket = context.socket(self.socket_type)
            try:
                if self.connection_type == ConnectionTypes.BIND:
                    self.socket.bind(self.endpoint)
                else:
                    self.socket.connect(self.endpoint)
                logger.info(f"Successfully connected ZeroMQ socket on {self.endpoint}.")
                return
            except Exception as e:
                logger.error(
                    f"Failed to setup ZeroMQ socket. Reason: {e} Retrying...{attempt}/{self.num_connect_retry}"
                )
                time.sleep(5)

        raise ConnectionError(f"Failed connecting to ZeroMQ socket after {self.num_connect_retry} attempts.")

    def close(self):
        try:
            self.socket.close()
        except Exception as e:
            logger.error(f"Failed to close socket {ZeroMQBase.name} at {self.endpoint}. Reason: {e}")

    def receive_message(self):
        pass

    def send(self, data):
        pass
