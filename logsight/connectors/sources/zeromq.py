import json
import logging
import time

import zmq

from connectors.zeromq_base import ZeroMQBase, ConnectionTypes

logger = logging.getLogger("logsight." + __name__)


class ZeroMQSubSource(ZeroMQBase):
    def __init__(self, endpoint: str, topic: str = "", private_key=None, application_name=None,
                 **kwargs):
        super().__init__(endpoint=endpoint, socket_type=zmq.SUB, connection_type=ConnectionTypes.CONNECT)
        if application_name and private_key:
            self.application_id = "_".join([private_key, application_name])
        else:
            self.application_id = ""
        self.topic = "_".join([self.application_id, topic]) if self.application_id else topic

    def connect(self):
        super().connect()
        logger.info(f"Subscribing to topic {self.topic}")
        topic_filter = self.topic.encode('utf8')
        self.socket.subscribe(topic_filter)

    def to_json(self):
        return {"source_type": "zeroMQSubSource", "endpoint": self.endpoint, "topic": self.topic}

    def receive_message(self):
        if not self.socket:
            raise ConnectionError("Socket is not connected. Please call connect() first.")
        try:
            topic_log = self.socket.recv().decode("utf-8")
            # TODO this should not happen here!
            log = json.loads(topic_log.split(" ", 1)[1])
        except Exception as e:
            logger.error(e)
            return None
        return log


class ZeroMQRepSource(ZeroMQBase):
    def __init__(self, endpoint: str):
        super().__init__(endpoint=endpoint, socket_type=zmq.REP, connection_type=ConnectionTypes.BIND)

    def receive_message(self):
        msg = self.socket.recv().decode("utf-8")
        return json.loads(msg)

    def connect(self):
        super().connect()

    def to_json(self):
        return {"source_type": "zeroMQRepSource", "endpoint": self.endpoint}
