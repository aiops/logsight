from time import sleep
from typing import Optional
from utils.fs import load_json
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from builders.application_builder import ApplicationBuilder
from config.global_vars import USES_KAFKA, USES_ES, PIPELINE_PATH
from multiprocessing import Process
import logging
from logsight_classes.data_class import AppConfig, PipelineConfig

logger = logging.getLogger("logsight." + __name__)


class Manager:
    def __init__(self, source, services, producer, topic_list=None, app_builder: ApplicationBuilder = None):
        self.source = source
        self.kafka_admin = services.get('kafka_admin', None) if USES_KAFKA else None
        self.elasticsearch_admin = services.get('elasticsearch_admin', None) if USES_ES else None
        self.producer = producer
        self.topic_list = topic_list or []
        self.db = services.get('database', None)

        self.active_apps = {}
        self.active_process_apps = {}
        self.app_builder = app_builder if app_builder else ApplicationBuilder(services)

        self.pipeline_config = PipelineConfig(**load_json(PIPELINE_PATH))

        for app in self.db.read_apps():
            self.create_application(AppConfig(**app))

    def create_topics_for_manager(self):
        for topic in self.topic_list:
            try:
                self.kafka_admin.create_topics(
                    [NewTopic(name=topic, num_partitions=1, replication_factor=1)])
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Created topic {topic}")
            except TopicAlreadyExistsError:
                logger.debug(f"Topic already exists with topic name: {topic}")
        logger.info("Created topics for manager.")

    def delete_topics_for_manager(self):
        for topic in self.topic_list:
            try:
                self.kafka_admin.delete_topics([topic])
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Deleted topic {topic}")
            except Exception as e:
                logger.error(e)
        logger.info("Deleted topics for manager.")

    def create_application(self, app_settings: AppConfig) -> Optional[dict]:
        if app_settings.application_id in self.active_apps.keys():
            return {"msg": f"Application {app_settings.application_id} already created"}

        logger.info(f"Building App {app_settings.application_name}.")
        app = self.app_builder.build_object(self.pipeline_config, app_settings)

        app_process = Process(target=start_process, args=(app,))
        self.active_apps[app_settings.application_id] = app
        self.active_process_apps[app_settings.application_id] = app_process
        logger.info("Starting app process")
        app_process.start()
        return app.to_json()

    def delete_application(self, app_settings: AppConfig) -> Optional[dict]:
        logger.info(f"Deleting application {app_settings.application_id}")

        app_process = self.active_process_apps[app_settings.application_id]
        app_process.terminate()

        application = self.active_apps[app_settings.application_id]
        if self.elasticsearch_admin:
            self.elasticsearch_admin.delete_indices(application.private_key, application.application_name)
        if self.kafka_admin:
            self.kafka_admin.delete_topics(application.topic_list)
        del self.active_apps[application.application_id]
        del self.active_process_apps[application.application_id]
        logger.info(f"Application successfully deleted with name: {application.application_name} "
                    f"and id: {application.application_id}")
        return {"msg": f"Application {app_settings.application_id} successfully deleted"}

    def run(self):
        self.start_listener()

    def start_listener(self):
        while self.source.has_next():
            msg = self.source.receive_message()
            logger.debug(f"Processing message {msg}")
            result = self.process_message(msg)
            if result:
                self.producer.send(result)

    def process_message(self, msg: dict) -> Optional[dict]:
        msg['application_id'] = str(msg['application_id'])
        app_settings = AppConfig(**msg)
        try:
            if app_settings.status.upper() == "CREATE":
                return self.create_application(app_settings)
            elif app_settings.status.upper() == 'DELETE':
                return self.delete_application(app_settings)
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
    logger.debug(f'Starting application {app}.')
    app.start()
    logger.debug(f"Application {app} Started.")
    while True:
        sleep(100)
