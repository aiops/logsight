import logging
from typing import Optional

import zmq
from tenacity import retry, stop_after_attempt, wait_fixed

from common.logsight_classes.mixins import DictMixin
from configs.global_vars import RETRY_ATTEMPTS, RETRY_TIMEOUT
from connectors.base.zeromq import ConnectionTypes, ZeroMQConnector
from connectors.serializers.serializers import DictSerializer
from connectors.sources import Source
from connectors.sources.source import ConnectableSource

logger = logging.getLogger("logsight." + __name__)


class ZeroMQSubSource(ZeroMQConnector, ConnectableSource):
    def __init__(self, endpoint: str, topic: str = None, connection_type: ConnectionTypes = ConnectionTypes.CONNECT,
                 serializer=DictSerializer()):
        ConnectableSource.__init__(self, serializer)
        ZeroMQConnector.__init__(self, endpoint=endpoint, socket_type=zmq.SUB, connection_type=connection_type)

        self.topic = topic

    def connect(self):
        ZeroMQConnector.connect(self)
        if self.topic:
            logger.info(f"Subscribing to topic {self.topic}")
        topic_filter = self.topic.encode('utf8')
        self.socket.subscribe(topic_filter)

    def to_dict(self):
        return {"source_type": "zeroMQSubSource", "endpoint": self.endpoint, "topic": self.topic}

    def _receive_message(self) -> Optional[dict]:
        if not self.socket:
            raise ConnectionError("Socket is not connected. Please call connect() first.")
        try:
            message = bytes(self.socket.recv())
            if self.topic:
                _, message = message.decode("utf-8").split(self.topic, 1)
            return message
        except Exception as e:
            logger.error(e)


class ZeroMQRepSource(ZeroMQConnector, Source, DictMixin):
    def __init__(self, endpoint: str):
        ZeroMQConnector.__init__(self, endpoint=endpoint, socket_type=zmq.REP,
                                 connection_type=ConnectionTypes.BIND)

    # noinspection PyUnresolvedReferences
    def _receive_message(self):
        msg = self.socket.recv().decode("utf-8")
        return msg

    @retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_fixed(RETRY_TIMEOUT))
    def connect(self):
        ZeroMQConnector.connect(self)

    def to_dict(self):
        return {"source_type": "zeroMQRepSource", "endpoint": self.endpoint}
