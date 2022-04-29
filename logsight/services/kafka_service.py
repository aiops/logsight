import logging
from time import sleep

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

logger = logging.getLogger("logsight." + __name__)


class KafkaService:
    def __init__(self, address: str, **_kwargs):
        logger.info(f"Connecting kafka admin client on {address}")
        tries = 5
        while tries > 0:
            tries = -1
            try:
                self.client = KafkaAdminClient(bootstrap_servers=address)
                break
            except Exception as e:
                logger.info(f"Failed to connect to kafka admin client on {address}. Reason: {e}. Retrying...")
                sleep(3)

    def create_topics_for_manager(self, topic_list):
        for topic in topic_list:
            try:
                self.create_topics(
                    [NewTopic(name=topic, num_partitions=1, replication_factor=1)])
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Created topic {topic}")
            except TopicAlreadyExistsError:
                logger.debug(f"Topic already exists with topic name: {topic}")
        logger.info("Created topics for manager.")

    def delete_topics_for_manager(self, topic_list):
        for topic in topic_list:
            try:
                self.delete_topics([topic])
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Deleted topic {topic}")
            except Exception as e:
                logger.error(e)

    def create_topics(self, topic):
        self.client.create_topics(topic)

    def delete_topics(self, topic):
        self.client.delete_topics(topic)
