from json import loads

from .base import StreamSource
import logging

from kafka import KafkaConsumer


class KafkaSource(StreamSource):
    """Data source - Kafka consumer.
    """

    def connect(self):
        self.kafka_source = KafkaConsumer(self.topic,
                                          bootstrap_servers=[self.address],
                                          auto_offset_reset=self.offset,
                                          group_id=self.group_id,
                                          api_version=(2, 0, 2),
                                          enable_auto_commit=True,
                                          value_deserializer=lambda x: loads(x.decode('utf-8'))
                                          )

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

        try:
            self.kafka_source = KafkaConsumer(self.topic,
                                              bootstrap_servers=[address],
                                              auto_offset_reset=offset,
                                              group_id=group_id,
                                              api_version=(2, 0, 2),
                                              enable_auto_commit=True,
                                              value_deserializer=lambda x: loads(x.decode('utf-8'))
                                              )
        except Exception as e:
            logger.error(e)

    def to_json(self):
        return {"topic": self.topic}

    def receive_message(self):
        return next(self.kafka_source).value

    def process_message(self):
        pass
