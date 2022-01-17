import logging
from time import sleep

import zmq

from .base import Source

logger = logging.getLogger("logsight." + __name__)


class ZeroMQSource(Source):
    """
    Data source - zeroMQ receiver
    """

    def __init__(self, topic: str, port: int = 5321):
        super().__init__()

        self.endpoint = f"tcp://*:{port}"
        self.topic = topic
        self.socket = None

    def connect(self):
        # Socket to talk to server
        context = zmq.Context()
        self.socket = context.socket(zmq.SUB)
        self.socket.setsockopt(zmq.SUBSCRIBE, self.topic)

        while True:
            logger.info(f"Setting up a zeroMQ SUB connection on {self.endpoint} with topic {self.topic}...")
            try:
                self.socket.connect(self.endpoint)
            except Exception as e:
                logger.error(f"Failed to setup a zeroMQ SUB connection to {self.endpoint}", e)
                sleep(5)
            break
        logger.info(
            f"Successfully connected to zeroMQ SUB on {self.endpoint}. Listening for messages on topic {self.topic}")

    def receive_message(self):
        msg = None
        try:
            msg = self.socket.recv_json()
        except Exception as e:
            logger.warning(
                f"Error occurred while receiving message from topic {self.topic} of endpoint {self.endpoint}", e)
        return msg

    def to_json(self):
        return {"connection": "zeroMQ", "endpoint": self.endpoint, "topic": self.topic}
