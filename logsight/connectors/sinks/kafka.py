import json
import logging
import time
from typing import Any, Optional

from kafka import KafkaProducer

from connectors.base.mixins import ConnectableSink

logger = logging.getLogger("logsight." + __name__)


class KafkaSink(ConnectableSink):

    def __init__(self, host: str, port: int, topic: str):
        """
        Init
        :param host: The hostname of the Kafka broker
        :type host: str
        :param port: The port number of the Kafka broker
        :type port: int
        :param topic: The name of the topic to which the data will be consumed
        :type topic: str
        """
        self.topic = topic
        self.address = f"{host}:{port}"
        self.kafka_sink = None

    def send(self, data: Any, target: Optional[Any] = None):
        """
          The send function sends a message to the Kafka topic specified in the
          constructor.  The message is sent as a JSON string

          Args:
              data: Send the data to kafka
              target: Specify the topic to which you want to send the data

          """
        topic = target or self.topic
        if not isinstance(data, list):
            data = [data]
        try:
            for d in data:
                self.kafka_sink.send(topic=topic, value=json.dumps(d).encode('utf-8'))
        except Exception as e:
            logger.error(e)

    def close(self):
        """
        Close the Kafka connection.
        """
        self.kafka_sink.close()

    def _connect(self):
        """
        The connect function is used to connect to the Kafka server. It will try
        to connect, and if it fails, it will wait 5 seconds and try again.

        Returns:
            A kafkaproducer object

        """
        try:
            self.kafka_sink = KafkaProducer(bootstrap_servers=self.address)

        except Exception as e:
            logger.error(f"Failed to connect to kafka consumer client on {self.address}. Reason: {e}. Retrying...")
            raise e
