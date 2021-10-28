import json
import logging

from kafka import KafkaProducer

from .base import Sink


class KafkaSink(Sink):
    def __init__(self, address: str, topic: str, application_id=None, **kwargs):
        """
        Args:
            address:
            topic:
            **kwargs:
        """
        super().__init__()
        self.application_id = application_id
        self.topic = "_".join([application_id, topic]) if application_id else topic

        logger = kwargs.get('logger', logging.getLogger('default'))
        logger.debug("Creating Kafka consumer")
        try:
            self.kafka_sink = KafkaProducer(bootstrap_servers=address)
        except Exception as e:
            logger.error(e)

    def send(self, data, topic=None):
        topic = topic or self.topic
        if not isinstance(data, list):
            data = [data]
        try:
            for d in data:
                self.kafka_sink.send(topic=topic, value=json.dumps(d).encode('utf-8'))
        except Exception as e:
            print(f"COULDNT SEND DATA TO SINK ON TOPIC {topic}")
