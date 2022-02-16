import logging

import zmq

from connectors.base.zeromq import ConnectionTypes, ZeroMQConnector
from connectors.sinks import Sink

logger = logging.getLogger("logsight." + __name__)


class ZeroMQPubSink(Sink, ZeroMQConnector):
    name = "zeroMQ pub sink"

    def __init__(self, endpoint: str, topic: str = "", retry_connect_num: int = 5, retry_timeout_sec: int = 5,
                 connection_type: ConnectionTypes = ConnectionTypes.CONNECT, **kwargs):
        ZeroMQConnector.__init__(self, endpoint, socket_type=zmq.PUB, connection_type=connection_type,
                                 retry_connect_num=retry_connect_num, retry_timeout_sec=retry_timeout_sec)
        self.topic = topic

    def connect(self):
        ZeroMQConnector.connect(self)

    def send(self, data: str):
        if not self.socket:
            raise ConnectionError("Socket is not connected. Please call connect() first.")

        if self.topic:
            msg = "%s %s" % (self.topic, data)
        else:
            msg = "%s" % data

        try:
            self.socket.send_string(msg)
        except Exception as e:
            logger.error(f"Error while sending message via {self.to_json()}. Reason: {e}")

    def to_json(self):
        return {"source_type": ZeroMQPubSink.name, "endpoint": self.endpoint, "topic": self.topic}
