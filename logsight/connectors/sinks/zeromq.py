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
from connectors.sources.source import Source
from connectors.zeromq_base import ZeroMQBase, ConnectionTypes

logger = logging.getLogger("logsight." + __name__)


class ZeroMQPubSink(ZeroMQBase):
    name = "zeroMQ pub sink"

    def __init__(self, endpoint: str, topic: str = "", private_key=None, application_name=None,
                 **kwargs):
        super().__init__(endpoint, socket_type=zmq.PUB, connection_type=ConnectionTypes.BIND)
        if application_name and private_key:
            self.application_id = "_".join([private_key, application_name])
        else:
            self.application_id = None
        self.topic = "_".join([self.application_id, topic, "control"]) if self.application_id else topic

    def connect(self):
        super().connect()

    def send(self, data: str):
        if self.topic:
            msg = b"%s %s" % (self.topic.encode(), data.encode())
        else:
            msg = b"%s" % (data.encode())

        try:
            self.socket.send(msg)
        except Exception as e:
            logger.error(f"Error while sending message via {self.to_json()}. Reason: {e}")

    def to_json(self):
        return {"source_type": ZeroMQPubSink.name, "endpoint": self.endpoint, "topic": self.topic}
