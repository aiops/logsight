import logging
from json import loads
from time import sleep
from typing import Optional

# noinspection PyPackageRequirements,PyProtectedMember
from kafka import KafkaConsumer as Consumer, TopicPartition

from connectors.serializers.base import LogBatchSerializer
from connectors.sources.source import LogBatchConnectableSource

logger = logging.getLogger("logsight." + __name__)


class KafkaSource(LogBatchConnectableSource):
    """Data source - a wrapper around a Kafka consumer that allows us to receive messages from a Kafka topic"""

    def __init__(self, host: str, port: int, topic: str, group_id: int = None, offset: str = 'earliest',
                 serializer: Optional[LogBatchSerializer] = None):
        """
        Args:
            host:str: Specify the host of the kafka server
            port:int: Specify the port of the kafka server
            topic:str: Specify the topic to subscribe to
            group_id:int=None: Set the group_id for the consumer
            offset:str='earliest': Set the offset of the consumer group to read from
            serializer:Optional[Serializer]=None: Specify the serializer that should be used to encode and decode messages
        """

        super().__init__(serializer)
        self.topic = topic
        self.address = f"{host}:{port}"
        self.offset = offset
        self.group_id = group_id
        self.kafka_source = None

        self._first_message = True

    def _connect(self):
        """The connect function creates a Kafka consumer client that connects to the specified bootstrap server and topic.
        It also sets the offset policy for the consumer, which is set to 'earliest' by default. The function will retry
        until it can successfully connect.

        Args:
          self: Access the attributes and methods of the class in python

        Returns:

        """
        logger.info(f"Creating kafka consumer via bootstrap server {self.address} for topic {self.topic} " +
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

    def close(self):
        """ Close the connection."""
        self.kafka_source.close()

    def _log_current_offset(self):
        """
        The _log_current_offset function logs the current offset of each partition in a topic.
        The function returns the offset of the last message in each partition.

        Args:
            self: Reference the object instance of the class

        Returns:
            The offset of the last message in each partition

        """
        partitions = []
        for partition in self.kafka_source.partitions_for_topic(self.topic):
            partitions.append(TopicPartition(self.topic, partition))

        end_offsets = self.kafka_source.end_offsets(partitions)
        logger.info(f"Current offset for topic {self.topic}: {end_offsets}.")

    def _receive_message(self) -> str:
        if self._first_message:
            self._first_message = False
            self._log_current_offset()
        return next(self.kafka_source).value
