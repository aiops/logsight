import json
import sys
import threading

from kafka.admin import NewTopic
from application import Application, AppBuilder
from config.globals import USES_KAFKA, USES_ES
from multiprocessing import Process
import logging

logger = logging.getLogger("logsight." + __name__)


class Manager:
    def __init__(self, source, services, producer, topic_list=None):
        self.source = source
        self.kafka_admin = services.get('kafka_admin', None) if USES_KAFKA else None
        self.elasticsearch_admin = services.get('elasticsearch_admin', None) if USES_ES else None
        self.producer = producer
        self.topic_list = topic_list or []

        self.active_apps = {}
        self.active_process_apps = {}

    def create_topics_for_manager(self):
        for topic in self.topic_list:
            try:
                self.kafka_admin.create_topics(
                    [NewTopic(name=topic, num_partitions=1, replication_factor=1)])
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Created topic {topic}")
            except Exception as e:
                logger.error(f"Topic already exists with topic name: {topic}")
        logger.info("Created topics for manager.")

    def delete_topics_for_manager(self):
        for topic in self.topic_list:
            try:
                self.kafka_admin.delete_topics([topic])
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Deleted topic {topic}")
            except Exception as e:
                logger.error(e)

        logger.info("Created topics for manager.")

    def create_application(self, app_settings):
        if app_settings['application_id'] in self.active_apps.keys():
            return {"msg": f"Application {app_settings['application_id']} already created"}
        logger.info("[MANAGER] Building App %s.", app_settings['application_id'])
        application = AppBuilder(self.kafka_admin, self.elasticsearch_admin).build_app(app_settings)
        app_process = Process(target=start_process, args=(application,))
        self.active_apps[app_settings['application_id']] = application
        self.active_process_apps[app_settings['application_id']] = app_process
        logger.info("Starting app process")
        app_process.start()
        app_json = application.to_json()
        logger.info(app_json)
        return app_json

    def delete_application(self, msg):
        logger.info(f"Deleting application {msg['application_id']}")
        app_process = self.active_process_apps[msg['application_id']]
        app_process.terminate()
        application = self.active_apps[msg['application_id']]
        if self.elasticsearch_admin:
            self.elasticsearch_admin.create_indices(application.application_id)
        if self.kafka_admin:
            self.kafka_admin.delete_topics(application.topic_list)
        del self.active_apps[application.application_id]
        del self.active_process_apps[application.application_id]
        return {"msg": f"Deleted application {application.application_id}"}

    def run(self):
        logger.info("Starting manager.")
        self.start_listener()
        #thrd = Process(name="MngrSrc", target=self.start_listener)
        #thrd.start()
        logger.info("Manager started.")
        #thrd.join()

    def start_listener(self):
        while self.source.has_next():
            msg = self.source.receive_message()
            logger.debug(f"[Manager] Processing message {msg}")
            result = self.process_message(msg)
            self.producer.send(result)

    def process_message(self, msg):
        msg['application_id'] = str(msg['application_id'])
        status = msg.get("status", "")
        try:
            if status.upper() == "CREATE":
                return self.create_application(msg)
            elif status.upper() == 'DELETE':
                return self.delete_application(msg)
            else:
                return {"msg": "Invalid application status"}
        except Exception as e:
            logger.error(e)

    def setup(self):
        if self.kafka_admin:
            self.delete_topics_for_manager()
            self.create_topics_for_manager()

        self.source.connect()


def start_process(app):
    logger.info(f'Starting application {app}.')
    app.start()
    logger.info(f"Application {app} Started.")
