import json
import logging
import sys
import time
from abc import abstractmethod
from enum import Enum
from typing import Optional

import zmq
from zmq import Socket

from connectors.sinks import Sink
from connectors.sources.base import Source
from connectors.zeromq_base import ZeroMQBase, ConnectionTypes

logger = logging.getLogger("logsight." + __name__)


class ZeroMQSubSource(Sink, ZeroMQBase):
    def __init__(self, endpoint: str, topic: str = "", private_key=None, application_name=None,
                 **kwargs):
        super(Source).__init__()
        super(ZeroMQBase, self).__init__(endpoint, socket_type=zmq.SUB, connection_type=ConnectionTypes.CONNECT)
        if application_name and private_key:
            self.application_id = "_".join([private_key, application_name])
        else:
            self.application_id = None
        self.topic = "_".join([self.application_id, topic]) if self.application_id else topic
        self.endpoint = endpoint
        self.socket: Optional[Socket] = None

    def connect(self):
        super().connect()
        logger.info(f"Subscribing to topic {self.topic}")
        topic_filter = self.topic.encode('utf8')
        self.socket.subscribe(topic_filter)

    def to_json(self):
        return {"source_type": "zeroMQ", "endpoint": self.endpoint, "topic": self.topic}

    def receive_message(self):
        if not self.socket:
            raise Exception("Socket is not connected. Please call connect() first.")
        try:
            topic_log = self.socket.recv().decode("utf-8")
            log = json.loads(topic_log.split(" ", 1)[1])
        except Exception as e:
            logger.error(e)
            return None
        return log

