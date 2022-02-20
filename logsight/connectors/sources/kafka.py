import logging
from json import loads
from time import sleep

from kafka import KafkaConsumer as Consumer, TopicPartition

from .source import StreamSource

logger = logging.getLogger("logsight." + __name__)


class KafkaSource(Source):
    """Data source - Kafka consumer.
    """

    def close(self):
        self.kafka_source.close()

    def __init__(self, address: str, topic: str, group_id: int = None, offset: str = 'earliest', private_key=None,
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
        if application_name and private_key:
            self.application_id = "_".join([private_key, application_name])
        else:
            self.application_id = None
        self.topic = "_".join([self.application_id, topic]) if self.application_id else topic
        self.address = address
        self.offset = offset
        self.group_id = group_id
        self.kafka_source = None

        self._first_message = True

    def connect(self):
        logger.info(f"Creating kafka consumer via bootstarp server {self.address} for topic {self.topic} " +
                    f"with offset policy '{self.offset}'.")
        while True:
            try:
                self.kafka_source = Consumer(
                    self.topic,
                    bootstrap_servers=[self.address],
                    auto_offset_reset=self.offset,
                    group_id=self.topic,
                    api_version=(2, 0, 2),
                    enable_auto_commit=True,
                    value_deserializer=lambda x: loads(x.decode('utf-8')),
                    auto_commit_interval_ms=1000
                )
            except Exception as e:
                logger.info(f"Failed to connect to kafka consumer client on {self.address}. Reason: {e}. Retrying...")
                sleep(5)
                continue
            break
        self._log_current_offset()

    def _log_current_offset(self):
        partitions = []
        for partition in self.kafka_source.partitions_for_topic(self.topic):
            partitions.append(TopicPartition(self.topic, partition))

        end_offsets = self.kafka_source.end_offsets(partitions)
        logger.info(f"Current offset for topic {self.topic}: {end_offsets}.")

    def to_json(self):
        return {"topic": self.topic}

    def receive_message(self):
        if self._first_message:
            self._first_message = False
            self._log_current_offset()
        return next(self.kafka_source).value
