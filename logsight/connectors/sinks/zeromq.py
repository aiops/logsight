import logging
from typing import Any, Optional

import zmq

from connectors.base import Sink
from connectors.connectors.zeromq.conn_types import ConnectionTypes
from connectors.connectors.zeromq.connector import ZeroMQConnector, ZeroMQConfigProperties

logger = logging.getLogger("logsight." + __name__)


class ZeroMQPubSink(Sink, ZeroMQConnector):
    name = "zeroMQ pub sink"

    def __init__(self, config: ZeroMQConfigProperties):
        config.socket_type = zmq.PUB
        print(config)

        ZeroMQConnector.__init__(self, config)
        self.topic = config.topic

    def send(self, data: Any, target: Optional[str] = None):
        topic = target if target else self.topic
        if not self.socket:
            raise ConnectionError("Socket is not connected. Please call connect() first.")

        msg = "%s %s" % (topic, data) if topic else "%s" % data
        try:
            self.socket.send_string(msg)
        except Exception as e:
            logger.error(f"Error while sending message via {self}. Reason: {e}")
