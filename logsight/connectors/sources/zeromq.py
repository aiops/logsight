import json
import logging
from typing import Optional

import zmq
from tenacity import retry, stop_after_attempt, wait_fixed

from connectors.base.zeromq import ConnectionTypes, ZeroMQConnector
from connectors.sources import Source
from connectors.transformers.transformers import DictTransformer
from common.logsight_classes.mixins import DictMixin

logger = logging.getLogger("logsight." + __name__)


# noinspection PyUnresolvedReferences
class ZeroMQSubSource(ZeroMQConnector, Source, DictMixin):
    def __init__(self, endpoint: str, topic: str = "", private_key=None, application_name=None,
                 connection_type: ConnectionTypes = ConnectionTypes.CONNECT, transformer=DictTransformer()):
        Source.__init__(self, transformer)
        ZeroMQConnector.__init__(self, endpoint=endpoint, socket_type=zmq.SUB, connection_type=connection_type)
        if application_name and private_key:
            self.application_id = "_".join([private_key, application_name])
        else:
            self.application_id = ""
        self.topic = "_".join([self.application_id, topic]) if self.application_id else topic

    def connect(self):
        ZeroMQConnector.connect(self)
        logger.info(f"Subscribing to topic {self.topic}")
        topic_filter = self.topic.encode('utf8')
        self.socket.subscribe(topic_filter)

    def to_dict(self):
        return {"source_type": "zeroMQSubSource", "endpoint": self.endpoint, "topic": self.topic}

    def _receive_message(self) -> Optional[dict]:
        if not self.socket:
            raise ConnectionError("Socket is not connected. Please call connect() first.")
        try:
            topic_log = self.socket.recv().decode("utf-8")
            message = topic_log.split(" ", 1)[1]
            log = json.loads(message)

        except Exception as e:
            logger.error(e)
            return
        return log


# noinspection PyUnresolvedReferences
class ZeroMQRepSource(ZeroMQConnector, Source, DictMixin):
    def __init__(self, endpoint: str):
        ZeroMQConnector.__init__(self, endpoint=endpoint, socket_type=zmq.REP,
                                 connection_type=ConnectionTypes.BIND)

    def _receive_message(self):
        msg = self.socket.recv().decode("utf-8")
        return msg

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(5))
    def connect(self):
        ZeroMQConnector.connect(self)

    def to_dict(self):
        return {"source_type": "zeroMQRepSource", "endpoint": self.endpoint}
