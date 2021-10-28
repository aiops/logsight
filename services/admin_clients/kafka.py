from kafka import KafkaAdminClient


class KafkaAdmin:
    def __init__(self, address, **kwargs):
        self.client = KafkaAdminClient(bootstrap_servers=address)

    def create_topics(self, topic):
        self.client.create_topics(topic)

    def delete_topics(self, topic):
        self.client.delete_topics(topic)
