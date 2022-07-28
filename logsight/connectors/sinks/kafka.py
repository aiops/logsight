import json
import logging
from typing import Any, Optional

from kafka import KafkaProducer

from logsight.connectors import Sink
from logsight.connectors.connectors.kafka import KafkaConfigProperties, KafkaConnector

logger = logging.getLogger("logsight." + __name__)


class KafkaSink(KafkaConnector, Sink):

    def __init__(self, config: KafkaConfigProperties):
        KafkaConnector.__init__(self, config)

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
                self.conn.send(topic=topic, value=json.dumps(d).encode('utf-8'))
        except Exception as e:
            logger.error(e)

    def _connect(self):
        """
        The connect function is used to connect to the Kafka server. It will try
        to connect, and if it fails, it will wait 5 seconds and try again.

        Returns:
            A kafkaproducer object

        """
        try:
            self.conn = KafkaProducer(bootstrap_servers=self.address)

        except Exception as e:
            logger.error(f"Failed to connect to kafka consumer client on {self.address}. Reason: {e}. Retrying...")
            raise e
