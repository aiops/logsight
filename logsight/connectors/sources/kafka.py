import logging
from json import loads
from time import sleep

from kafka import KafkaConsumer

from .base import StreamSource


class KafkaSource(StreamSource):
    """Data source - Kafka consumer.
    """

    def connect(self):
        """No explicit connect method for kafka"""
        pass

    def __init__(self, address: str, topic: str, group_id: int = None, offset: str = 'latest', private_key=None,
                 application_name=None, **kwargs):
        """
        Args:
            address:
            topic:
            group_id:
            offset:
            **kwargs:
        """
        super().__init__()
        logger = kwargs.get('logger', logging.getLogger('default'))
        logger.debug("Creating Kafka consumer")
        if application_name and private_key:
            self.application_id = "_".join([private_key, application_name])
        else:
            self.application_id = None
        self.topic = "_".join([self.application_id, topic]) if self.application_id else topic
        self.address = address
        self.offset = offset
        self.group_id = group_id

        while True:
            try:
                self.kafka_source = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=[self.address],
                    auto_offset_reset=self.offset,
                    group_id=self.group_id,
                    api_version=(2, 0, 2),
                    enable_auto_commit=True,
                    value_deserializer=lambda x: loads(x.decode('utf-8'))
                )
            except Exception as e:
                logger.info(f"Failed to connect to kafka consumer client on {address}. Reason: {e}. Retrying...")
                sleep(5)
                continue
            break

    def to_json(self):
        return {"topic": self.topic}

    def receive_message(self):
        return next(self.kafka_source).value
