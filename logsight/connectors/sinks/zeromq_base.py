import logging
import time
from enum import Enum
from typing import Optional

import zmq
from zmq import Socket

from connectors.sinks import Sink

logger = logging.getLogger("logsight." + __name__)


class SinkConnectionTypes(Enum):
    BIND = 1
    CONNECT = 2


class ZeroMQBase(Sink):
    name = "zeromq"

    def __init__(self, endpoint: str, socket_type: zmq.constants,
                 connection_type: SinkConnectionTypes, retry_connect_num: int = 5,
                 retry_timeout_sec: int = 5):
        super().__init__()
        self.endpoint = endpoint
        self.socket_type = socket_type
        self.num_connect_retry = retry_connect_num
        self.socket: Optional[Socket] = None
        self.connection_type = connection_type
        self.retry_timeout_sec = retry_timeout_sec

    def connect(self):
        attempt = 0
        while attempt < self.num_connect_retry:
            attempt += 1
            logger.info(f"Setting up ZeroMQ socket on {self.endpoint}.")
            context = zmq.Context()
            self.socket = context.socket(self.socket_type)
            self.socket.set_hwm(0)
            try:
                if self.connection_type == SinkConnectionTypes.BIND:
                    self.socket.bind(self.endpoint)
                elif self.connection_type == SinkConnectionTypes.CONNECT:
                    self.socket.connect(self.endpoint)
                else:
                    raise ConnectionError(
                        f"Invalid connection type. Use one of "
                        f"[{SinkConnectionTypes.CONNECT.name}, {SinkConnectionTypes.BIND.name}]"
                    )
                logger.info(f"Successfully connected ZeroMQ {self.connection_type.name} socket on {self.endpoint}.")
                return
            except Exception as e:
                logger.error(
                    f"Failed to setup ZeroMQ socket. Reason: {e} Retrying...{attempt}/{self.num_connect_retry}"
                )
                time.sleep(self.retry_timeout_sec)

        raise ConnectionError(f"Failed connecting to ZeroMQ socket after {self.num_connect_retry} attempts.")

    def close(self):
        if self.socket:
            try:
                self.socket.close()
            except Exception as e:
                logger.error(f"Failed to close socket {ZeroMQBase.name} at {self.endpoint}. Reason: {e}")

    def receive_message(self):
        pass

    def send(self, data):
        pass
