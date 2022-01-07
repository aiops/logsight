import json
import logging
import time

from kafka import KafkaProducer

from .base import Sink

logger = logging.getLogger("logsight." + __name__)


class KafkaSink(Sink):
    def __init__(self, address: str, topic: str, private_key=None, application_name=None, **kwargs):
        """
        Args:
            address:
            topic:
            **kwargs:
        """
        super().__init__(**kwargs)
        if application_name and private_key:
            self.application_id = "_".join([private_key, application_name])
        else:
            self.application_id = None
        self.topic = "_".join([self.application_id, topic]) if self.application_id else topic
        self.address = address

        self.kafka_sink = None
        self.connect()

    def connect(self):
        logger.debug("Creating Kafka producer")
        while True:
            try:
                self.kafka_sink = KafkaProducer(bootstrap_servers=self.address)
            except Exception as e:
                logger.info(f"Failed to connect to kafka consumer client on {self.address}. Reason: {e}. Retrying...")
                time.sleep(5)
                continue
            break

    def send(self, data, topic=None):
        topic = topic or self.topic
        if not isinstance(data, list):
            data = [data]
        try:
            for d in data:
                self.kafka_sink.send(topic=topic, value=json.dumps(d).encode('utf-8'))
        except Exception as e:
            logger.error(e)
