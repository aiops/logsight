from json import loads

from .base import StreamSource
import logging

from kafka import KafkaConsumer


class KafkaSource(StreamSource):
    """Data source - Kafka consumer.
    """

    def connect(self):
        pass

    def __init__(self, address: str, topic: str, group_id: int = None, offset: str = 'latest', application_id=None, **kwargs):
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

        topic = "_".join([application_id, topic]) if application_id else topic
        self.topic = topic
        self.address = address

        try:
            self.kafka_source = KafkaConsumer(topic,
                                              bootstrap_servers=[address],
                                              auto_offset_reset=offset,
                                              group_id=group_id,
                                              api_version=(2, 0, 2),
                                              enable_auto_commit=True,
                                              value_deserializer=lambda x: loads(x.decode('utf-8'))
                                              )
        except Exception as e:
            logger.error(e)

    def receive_message(self):
        return next(self.kafka_source).value

    def process_message(self):
        pass
