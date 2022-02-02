import json
import logging
import sys
import time

import zmq

from connectors.sources.base import StreamSource

logger = logging.getLogger("logsight." + __name__)


class ZeroMQ(StreamSource):
    """Data source - Kafka consumer.
    """

    def __init__(self, endpoint: str, topic: str, private_key=None, application_name=None, **kwargs):
        """
        Args:
            address:
            topic:
            **kwargs:
        """
        super().__init__()
        if application_name and private_key:
            self.application_id = "_".join([private_key, application_name])
        else:
            self.application_id = None
        self.topic = "_".join([self.application_id, topic]) if self.application_id else topic
        self.endpoint = endpoint
        self.socket = None

    def connect(self):
        while True:
            logger.info(f"Setting up ZeroMQ socket on {self.endpoint}.")
            context = zmq.Context()
            self.socket = context.socket(zmq.SUB)
            try:
                self.socket.connect(self.endpoint)
            except Exception as e:
                logger.error(f"Failed to setup ZeroMQ socket. Reason: {e} Retrying...")
                time.sleep(5)
            logger.info(f"Subscribing to topic {self.topic}")
            topic_filter = self.topic.encode('utf8')
            self.socket.subscribe(topic_filter)
            break

    def to_json(self):
        return {"endpoint": self.endpoint, "topic": self.topic}

    def receive_message(self):
        try:
            topic_log = self.socket.recv().decode("utf-8")
            log = json.loads(topic_log.split(" ", 1)[1])
            print(log)
        except Exception:
            return None
        return log
