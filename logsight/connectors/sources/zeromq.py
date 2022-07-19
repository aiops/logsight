import logging

import zmq

from common.logsight_classes.mixins import DictMixin
from connectors.base.mixins import ConnectableSource
from connectors.connectors.zeromq import ConnectionTypes, ZeroMQConnector
from connectors.serializers import JSONSerializer

logger = logging.getLogger("logsight." + __name__)


class ZeroMQSubSource(ConnectableSource, ZeroMQConnector):
    def __init__(self, endpoint: str, topic: str = None, connection_type: ConnectionTypes = ConnectionTypes.CONNECT):
        super(ZeroMQSubSource, self).__init__(endpoint=endpoint, socket_type=zmq.SUB, connection_type=connection_type)
        self.topic = topic

    def _connect(self):
        ZeroMQConnector._connect(self)
        if self.topic:
            logger.info(f"Subscribing to topic {self.topic}")
        topic_filter = self.topic.encode('utf8')
        self.socket.subscribe(topic_filter)

    def receive_message(self) -> str:
        if not self.socket:
            raise ConnectionError("Socket is not connected. Please call connect() first.")
        try:
            msg = bytes(self.socket.recv()).decode("utf-8")
            if self.topic:
                _, msg = msg.split(self.topic, 1)
            return msg
        except Exception as e:
            logger.error(e)


class ZeroMQRepSource(ZeroMQConnector, ConnectableSource):
    def __init__(self, endpoint: str):
        ZeroMQConnector.__init__(self, endpoint=endpoint, socket_type=zmq.REP, connection_type=ConnectionTypes.BIND)

    def receive_message(self) -> str:
        return bytes(self.socket.recv()).decode("utf-8")
