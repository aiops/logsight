import logging

import zmq

from logsight.connectors.base.mixins import ConnectableSource
from logsight.connectors.connectors.zeromq import ZeroMQConfigProperties
from logsight.connectors.connectors.zeromq.connector import ConnectionTypes, ZeroMQConnector

logger = logging.getLogger("logsight." + __name__)


class ZeroMQSubSource(ConnectableSource, ZeroMQConnector):
    def __init__(self, config: ZeroMQConfigProperties):
        config.socket_type = zmq.SUB
        config.connection_type = ConnectionTypes.CONNECT
        ZeroMQConnector.__init__(self, config)
        self.topic = config.topic

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
    def __init__(self, config: ZeroMQConfigProperties):
        config.socket_type = zmq.REP
        config.connection_type = ConnectionTypes.BIND
        ZeroMQConnector.__init__(self, config)

    def receive_message(self) -> str:
        return bytes(self.socket.recv()).decode("utf-8")
