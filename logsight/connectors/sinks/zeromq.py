import logging

import zmq

from .zeromq_base import ZeroMQBase, ConnectionTypes

logger = logging.getLogger("logsight." + __name__)


class ZeroMQPubSink(ZeroMQBase):
    name = "zeroMQ pub sink"

    def __init__(self, endpoint: str, topic: str = "", private_key=None, application_name=None,
                 retry_connect_num: int = 5, retry_timeout_sec: int = 5, **kwargs):
        super().__init__(endpoint, socket_type=zmq.PUB, connection_type=ConnectionTypes.CONNECT,
                         retry_connect_num=retry_connect_num, retry_timeout_sec=retry_timeout_sec)
        if application_name and private_key:
            self.application_id = "_".join([private_key, application_name])
        else:
            self.application_id = ""
        self.topic = "_".join([self.application_id, topic]) if self.application_id else topic

    def connect(self):
        super().connect()

    def send(self, data: str):
        if not self.socket:
            raise ConnectionError("Socket is not connected. Please call connect() first.")

        if self.topic:
            msg = "%s %s" % (self.topic, data)
        else:
            msg = "%s" % data

        try:
            self.socket.send_string(msg)
        except Exception as e:
            logger.error(f"Error while sending message via {self.to_json()}. Reason: {e}")

    def to_json(self):
        return {"source_type": ZeroMQPubSink.name, "endpoint": self.endpoint, "topic": self.topic}
