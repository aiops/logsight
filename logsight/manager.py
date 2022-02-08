import json
import logging
from multiprocessing import Process
from typing import Optional
from uuid import UUID

from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from builders.application_builder import ApplicationBuilder
from config.global_vars import USES_KAFKA, USES_ES, PIPELINE_PATH
from logsight_classes.application import Application
from logsight_classes.data_class import AppConfig, PipelineConfig
from logsight_classes.responses import ErrorResponse, SuccessResponse, Response
from utils.fs import load_json
from http import HTTPStatus
# set_start_method("fork")

logger = logging.getLogger("logsight." + __name__)


class Manager:
    def __init__(self, source, services, producer, topic_list=None, app_builder: ApplicationBuilder = None):
        self.source = source
        self.kafka_admin = services.get('kafka_admin', None) if USES_KAFKA else None
        self.elasticsearch_admin = services.get('elasticsearch_admin', None) if USES_ES else None
        self.producer = producer
        self.topic_list = topic_list or []
        self.db = services.get('database', None)
        # self.app_pool = Pool(3)
        self.active_apps = {}
        self.active_process_apps = {}
        self.app_builder = app_builder if app_builder else ApplicationBuilder(services)

        self.pipeline_config = PipelineConfig(**load_json(PIPELINE_PATH))
        if self.db:
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

    def create_application(self, app_settings: AppConfig) -> Response:
        app_settings.action = ''
        if app_settings.application_id in self.active_apps.keys():
            return ErrorResponse(message = f"Application {app_settings.application_id} already created", status=HTTPStatus.CONFLICT)

        logger.info(f"Building App {app_settings.application_name}.")
        app = self.app_builder.build_object(self.pipeline_config, app_settings)
        app_process = Process(target=start_process, args=(app,))
        self.active_apps[app_settings.application_id] = app
        self.active_process_apps[app_settings.application_id] = app_process
        logger.info("Starting app process")
        app_process.start()
        # app.start()
        # e.wait()
        return SuccessResponse(message = f"Application {app.application_id} CREATED")

    def delete_application(self, application_id) -> Response:
        logger.info(f"Deleting application {application_id}")
        if application_id not in self.active_apps.keys():
            logger.info(
                f"Application {application_id} does not exists in the active apps, therefore cannot be deleted.!")
            return ErrorResponse(message = f"Application {application_id} does not exists in the active apps, therefore cannot be deleted.!", status=HTTPStatus.CONFLICT)

        app_process = self.active_process_apps[application_id]
        app_process.terminate()
        application = self.active_apps[application_id]
        if self.elasticsearch_admin:
            self.elasticsearch_admin.delete_indices(application.private_key, application.application_name)
        if self.kafka_admin:
            self.kafka_admin.delete_topics(application.topic_list)
        logger.info("Application object obtained from active apps.")
        del self.active_apps[application_id]
        logger.info("Application object deleted from active apps.")
        del self.active_process_apps[application_id]
        logger.info("Application object deleted from active_process_apps apps.")
        logger.info(f"Application successfully deleted with name: {application.application_name} "
                    f"and id: {application.application_id}")
        return SuccessResponse(message=f"Application {application_id} DELETED")

    def run(self):
        self.start_listener()

    def start_listener(self):
        while self.source.has_next():
            msg = self.source.receive_message()
            logger.debug(f"Processing message {msg}")
            try:
                result = self.process_message(msg)
                self.source.socket.send(json.dumps(result.dict(), indent=2).encode('utf-8'))
                logger.info(f"Result sent: {str(json.dumps(result.dict(), indent=2).encode('utf-8'))}")
            except Exception as e:
                self.source.socket.send(json.dumps(ErrorResponse(str(e)).dict(), indent=2).encode('utf-8'))

    def process_message(self, msg: dict) -> Response:
        try:
            app_settings = AppConfig(application_id=UUID(msg.get("id")),
                                 application_name=msg.get("name"),
                                 private_key=msg.get("userKey"),
                                 action=msg.get("action"))
        except Exception as e:
            return ErrorResponse(message = str(e), status = HTTPStatus.BAD_REQUEST)
        if app_settings.action.upper() == "CREATE":
            return self.create_application(app_settings)
        elif app_settings.action.upper() == 'DELETE':
            return self.delete_application(app_settings.application_id)
        else:
            return ErrorResponse(message='Invalid application status. Application status needs to be one of ["CREATE", "DELETE"])', status=HTTPStatus.BAD_REQUEST)


    def setup(self):
        if self.kafka_admin:
            self.delete_topics_for_manager()
            self.create_topics_for_manager()
        self.source.connect()


def start_process(app: Application):
    logger.debug(f'Starting application {app}.')
    app.start()
    logger.debug(f"Application {app} Started.")
