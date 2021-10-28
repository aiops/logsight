import json
import threading

from kafka.admin import NewTopic
from application import Application, AppBuilder
from config.globals import USES_KAFKA, USES_ES


class Manager:
    def __init__(self, source, services, producer, topic_list=None):
        self.source = source
        self.kafka_admin = services.get('kafka_admin', None) if USES_KAFKA else None
        self.elasticsearch_admin = services.get('elasticsearch_admin', None) if USES_ES else None
        self.producer = producer
        self.topic_list = topic_list or []

        self.active_apps = {}

    def create_topics_for_manager(self):
        for topic in self.topic_list:
            try:
                self.kafka_admin.create_topics(
                    [NewTopic(name=topic, num_partitions=1, replication_factor=1)])
                print("Created topic", topic)
            except Exception as e:
                print("Topic already exists with topic name", topic)

    def delete_topics_for_manager(self):
        for topic in self.topic_list:
            try:
                self.kafka_admin.delete_topics([topic])
                print("Deleted topic", topic)
            except Exception as e:
                print(e)

    def create_application(self, app_settings):
        if app_settings['application_id'] in self.active_apps.keys():
            return {"msg": f"Application {app_settings['application_id']} already created"}
        print("[MANAGER] Building App.")
        application = AppBuilder(self.kafka_admin, self.elasticsearch_admin).build_app(app_settings)
        self.active_apps[app_settings['application_id']] = application
        application.start()
        return {"msg": f"Created application {app_settings['application_id']}"}

    def delete_application(self, msg):
        application = self.active_apps[msg['application_id']]
        application.stop()
        if self.elasticsearch_admin:
            self.elasticsearch_admin.create_indices(application.application_id)
        if self.kafka_admin:
            self.kafka_admin.delete_topics(application.topic_list)
        del self.active_apps[application.application_id]
        return {"msg": f"Deleted application {application.application_id}"}

    def run(self):
        thrd = threading.Thread(target=self.start_listener)
        thrd.start()

        while True:
            pass

    def start_listener(self):
        while self.source.has_next():
            print("Getting message")
            msg = self.source.receive_message()
            print("got message")

            print(f"[Manager] Processing message {msg}")

            result = self.process_message(msg)
            self.producer.send(result)

    def process_message(self, msg):
        msg['application_id'] = str(msg['application_id'])
        status = msg.get("status", "")
        if status.upper() == "CREATE":
            return self.create_application(msg)
        elif status.upper() == 'DELETE':
            return self.delete_application(msg)
        else:
            return {"msg": "Invalid application status"}

    def setup(self):
        print(self.kafka_admin)
        if self.kafka_admin:
            self.delete_topics_for_manager()
            self.create_topics_for_manager()

        self.source.connect()
