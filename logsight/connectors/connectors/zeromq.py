import logging
from enum import Enum
from typing import Optional

import zmq
from zmq import Socket, Context

from connectors.base.mixins import ConnectableConnector

logger = logging.getLogger("logsight." + __name__)


class ConnectionTypes(Enum):
    BIND = 1
    CONNECT = 2


class ZeroMQConnector(ConnectableConnector):
    name = "zeromq"

    def __init__(self, endpoint: str, socket_type: zmq.constants,
                 connection_type: ConnectionTypes):
        self.endpoint = endpoint
        self.socket_type = socket_type
        self.context: Optional[Context] = None
        self.socket: Optional[Socket] = None
        self.connection_type = connection_type

    def _connect(self):
        logger.info(f"Setting up ZeroMQ socket on {self.endpoint}.")
        self.context = Context()
        self.socket = self.context.socket(self.socket_type)
        self.socket.set_hwm(8192)
        try:
            if self.connection_type == ConnectionTypes.BIND:
                self.socket.bind(self.endpoint)
            elif self.connection_type == ConnectionTypes.CONNECT:
                self.socket.connect(self.endpoint)
            else:
                raise ConnectionError(
                    f"Invalid connection type. Use one of "
                    f"[{ConnectionTypes.CONNECT.name}, {ConnectionTypes.BIND.name}]"
                )
            logger.info(f"Successfully connected ZeroMQ {self.connection_type.name} socket on {self.endpoint}.")
            return
        except Exception as e:
            logger.warning(
                f"Failed to setup ZeroMQ socket. Reason: {e} Retrying..."
            )
            self.close()
            raise e

    def close(self):
        if self.socket:
            try:
                self.context.destroy()
                self.socket.close()
            except Exception as e:
                logger.warning(f"Failed to close socket {self.name} at {self.endpoint}. Reason: {e}")
