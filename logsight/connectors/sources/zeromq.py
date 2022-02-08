import json
import logging
import sys
import time
from abc import abstractmethod
from enum import Enum
from typing import Optional

import zmq
from zmq import Socket

from connectors.sources.base import Source

logger = logging.getLogger("logsight." + __name__)


class ConnectionTypes(Enum):
    BIND = 1
    CONNECT = 2


class ZeroMQBase(Source):
    def __init__(self, endpoint: str, socket_type: zmq.constants,
                 connection_type: ConnectionTypes = ConnectionTypes.BIND):
        super(ZeroMQBase, self).__init__()
        self.endpoint = endpoint
        self.socket_type = socket_type
        self.max_retries = 5
        self.socket: Optional[Socket] = None
        self.connection_type = connection_type

    def connect(self):

        attempt = 0
        while attempt < self.max_retries:
            attempt += 1
            logger.info(f"Setting up ZeroMQ socket on {self.endpoint}.")
            context = zmq.Context()
            self.socket = context.socket(self.socket_type)
            try:
                if self.connection_type == ConnectionTypes.BIND:
                    self.socket.bind(self.endpoint)
                else:
                    self.socket.connect(self.endpoint)
                return
            except Exception as e:
                logger.error(f"Failed to setup ZeroMQ socket. Reason: {e} Retrying...{attempt}/{self.max_retries} ")
                time.sleep(5)

        raise Exception(f"Failed connecting to ZeroMQ socket after {self.max_retries} attempts.")

    @abstractmethod
    def receive_message(self):
        raise NotImplementedError


class ZeroMQSubSource(ZeroMQBase):
    def __init__(self, endpoint: str, topic: str = "", private_key=None, application_name=None,
                 **kwargs):
        super().__init__(endpoint, socket_type=zmq.SUB, connection_type=ConnectionTypes.CONNECT)
        if application_name and private_key:
            self.application_id = "_".join([private_key, application_name])
        else:
            self.application_id = None
        self.topic = "_".join([self.application_id, topic]) if self.application_id else topic
        self.endpoint = endpoint
        self.socket: Optional[Socket] = None
        self.max_retires = 5

    def connect(self):
        super().connect()
        logger.info(f"Subscribing to topic {self.topic}")
        topic_filter = self.topic.encode('utf8')
        self.socket.subscribe(topic_filter)

    def to_json(self):
        return {"endpoint": self.endpoint, "topic": self.topic}

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


class ZeroMQRepSource(ZeroMQBase):
    def __init__(self, endpoint: str):
        super(ZeroMQRepSource, self).__init__(endpoint, socket_type=zmq.REP, connection_type=ConnectionTypes.BIND)

    def receive_message(self):
        msg = self.socket.recv().decode("utf-8")
        return json.loads(msg)
