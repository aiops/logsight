import json
import logging
from time import sleep

from kafka import KafkaProducer

from .base import Sink


class KafkaSink(Sink):
    def __init__(self, address: str, topic: str, private_key=None, application_name=None, **kwargs):
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
        self.address = address
        # self.cnt = 0
        logger = kwargs.get('logger', logging.getLogger('default'))
        logger.debug("Creating Kafka producer")
        try:
            self.kafka_sink = KafkaProducer(bootstrap_servers=address)
        except Exception as e:
            logger.error(e)

    def connect(self):
        self.kafka_sink = KafkaProducer(bootstrap_servers=self.address)

    def send(self, data, topic=None):
        topic = topic or self.topic
        if not isinstance(data, list):
            data = [data]
        try:
            for d in data:
                # self.cnt += 1
                self.kafka_sink.send(topic=topic, value=json.dumps(d).encode('utf-8'))
                # if self.cnt % 100 == 0:
                #     print(f"Sent {self.cnt} logs to topic {self.topic}")
            # print(f"sent data on topic {topic}")
        except Exception as e:
            print(f"COULDNT SEND DATA TO SINK ON TOPIC {topic}")
            print(e)
