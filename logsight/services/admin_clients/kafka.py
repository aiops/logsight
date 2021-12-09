import logging
from time import sleep

from kafka import KafkaAdminClient

logger = logging.getLogger("logsight." + __name__)


class KafkaAdmin:
    def __init__(self, address: str, **_kwargs):
        logger.info(f"Connecting kafka admin client on {address}")
        while True:
            try:
                self.client = KafkaAdminClient(bootstrap_servers=address)
            except Exception as e:
                logger.info(f"Failed to connect to kafka admin client on {address}. Reason: {e}. Retrying...")
                sleep(5)
                continue
            break

    def create_topics(self, topic):
        self.client.create_topics(topic)

    def delete_topics(self, topic):
        self.client.delete_topics(topic)
