import logging
from typing import Optional

from zmq import Context, Socket

from logsight.connectors.base.mixins import ConnectableConnector
from .configuration import ZeroMQConfigProperties
from .conn_types import ConnectionTypes

logger = logging.getLogger("logsight." + __name__)


class ZeroMQConnector(ConnectableConnector):
    name = "zeromq"

    def __init__(self, config: ZeroMQConfigProperties):
        self.endpoint = config.endpoint
        self.socket_type = config.socket_type
        self.context: Optional[Context] = None
        self.socket: Optional[Socket] = None
        self.connection_type = config.connection_type

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
                self.context.destroy(linger=2)
                self.socket.close()
            except Exception as e:
                logger.warning(f"Failed to close socket {self.name} at {self.endpoint}. Reason: {e}")
