import logging
from typing import Any, Optional

import zmq

from connectors.base.zeromq import ConnectionTypes, ZeroMQConnector
from connectors.serializers import Serializer
from connectors.sinks import Sink

logger = logging.getLogger("logsight." + __name__)


class ZeroMQPubSink(Sink, ZeroMQConnector):
    name = "zeroMQ pub sink"

    def __init__(self, endpoint: str, topic: str = "", connection_type: ConnectionTypes = ConnectionTypes.CONNECT,
                 serializer: Optional[Serializer] = None):
        Sink.__init__(self, serializer)
        ZeroMQConnector.__init__(self, endpoint, socket_type=zmq.PUB, connection_type=connection_type)
        self.topic = topic

    def send(self, data: Any, target: Optional[str] = None):
        topic = target if target else self.topic
        if not self.socket:
            raise ConnectionError("Socket is not connected. Please call connect() first.")

        if topic:
            msg = "%s %s" % (topic, data)
        else:
            msg = "%s" % data

        try:
            self.socket.send_string(msg)
        except Exception as e:
            logger.error(f"Error while sending message via {self}. Reason: {e}")
