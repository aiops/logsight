from logsight.connectors import ConnectableConnector
from .configuration import KafkaConfigProperties


class KafkaConnector(ConnectableConnector):
    def __init__(self, config: KafkaConfigProperties):
        self.address = f"{config.host}:{config.port}"
        self.topic = config.topic
        self.conn = None

    def _connect(self):
        raise NotImplementedError

    def close(self):
        self.conn.close()
