import logging

# noinspection PyPackageRequirements,PyProtectedMember
from kafka import KafkaConsumer as Consumer, TopicPartition

from connectors.base.mixins import ConnectableSource
from connectors.connectors.kafka.configuration import KafkaConfigProperties
from connectors.connectors.kafka.connector import KafkaConnector

logger = logging.getLogger("logsight." + __name__)


class KafkaSource(KafkaConnector, ConnectableSource):
    """Data source - a wrapper around a Kafka consumer that allows us to receive messages from a Kafka topic"""

    def __init__(self, config: KafkaConfigProperties):
        super().__init__(config)
        self.offset = config.offset
        self.group_id = config.group_id
        self.conn = None

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
        try:
            self.conn = Consumer(
                self.topic,
                bootstrap_servers=self.address,
                auto_commit_interval_ms=1000,
                max_partition_fetch_bytes=5 * 1024 * 1024,
                group_id=self.topic,
                enable_auto_commit=True
            )
        except Exception as e:
            logger.info(f"Failed to connect to kafka consumer client on {self.address}. Reason: {e}. Retrying...")
            raise e
        logger.info(f"Connected to kafka on {self.address}")

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
        for partition in self.conn.partitions_for_topic(self.topic):
            partitions.append(TopicPartition(self.topic, partition))

        end_offsets = self.conn.end_offsets(partitions)
        logger.info(f"Current offset for topic {self.topic}: {end_offsets}.")

    def receive_message(self) -> str:
        if self._first_message:
            self._first_message = False
        return next(self.conn).value
