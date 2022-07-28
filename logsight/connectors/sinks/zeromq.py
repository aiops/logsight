import logging
from typing import Any, Optional

import zmq

from logsight.connectors.base import Sink
from logsight.connectors.connectors.zeromq.connector import ZeroMQConfigProperties, ZeroMQConnector

logger = logging.getLogger("logsight." + __name__)


class ZeroMQPubSink(Sink, ZeroMQConnector):
    name = "zeroMQ pub sink"

    def __init__(self, config: ZeroMQConfigProperties):
        config.socket_type = zmq.PUB

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
