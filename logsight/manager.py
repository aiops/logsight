import json
import logging
from http import HTTPStatus
from multiprocessing import Process

from builders.application_builder import ApplicationBuilder
from configs.global_vars import USES_ES, USES_KAFKA
from logsight_classes.application import Application
from logsight_classes.data_class import AppConfig
from logsight_classes.responses import ApplicationOperationResponse, ErrorApplicationOperationResponse, \
    SuccessApplicationOperationResponse
from modules.core.timer import NamedTimer
from services import ModulePipelineConfig
from utils.helpers import DataClassJSONEncoder

logger = logging.getLogger("logsight." + __name__)


class Manager:
    def __init__(self, source, services, producer, app_builder: ApplicationBuilder = None):
        self.source = source
        self.kafka_admin = services.get('kafka_admin', None) if USES_KAFKA else None
        self.elasticsearch_admin = services.get('elasticsearch_admin', None) if USES_ES else None
        self.producer = producer
        self.db = services.get('database', None)
        self.active_apps = {}
        self.active_process_apps = {}
        self.app_builder = app_builder if app_builder else ApplicationBuilder(services)

        self.pipeline_config = ModulePipelineConfig()
        self.sync_timer = None
        if self.db:
            self.sync_timer = NamedTimer(timeout_period=600, callback=self._sync_apps, name="Sync app with db")
            self.sync_timer.start()
            for app in self.db.read_apps():
                self.create_application(AppConfig(**app))

    def create_application(self, app_settings: AppConfig) -> ApplicationOperationResponse:
        app_settings.action = ''
        if app_settings.application_id in self.active_apps.keys():
            return SuccessApplicationOperationResponse(
                app_id=app_settings.application_id,
                message=f"Application {app_settings.application_id} already created."
            )

        logger.info(f"Building App {app_settings.application_name}.")
        app = self.app_builder.build_object(self.pipeline_config.pipeline_config, app_settings)
        app_process = Process(target=start_process, args=(app,))
        self.active_apps[app_settings.application_id] = app
        self.active_process_apps[app_settings.application_id] = app_process
        logger.info("Starting app process")
        app_process.start()
        # time.sleep()  # TODO this needs to be fixed.
        # app.start()
        # e.wait()
        return SuccessApplicationOperationResponse(app_id=str(app.application_id),
                                                   message=f"Application {app.application_id} CREATED")

    def delete_application(self, application_id) -> ApplicationOperationResponse:
        logger.info(f"Deleting application {application_id}")
        if application_id not in self.active_apps.keys():
            logger.info(
                f"Application {application_id} does not exists in the active apps, therefore cannot be deleted.!")
            return SuccessApplicationOperationResponse(
                app_id=str(application_id),
                message=f"Application {application_id} does not exists in the active apps, therefore cannot be deleted.!x§  ",
            )

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
        return SuccessApplicationOperationResponse(app_id=str(application_id),
                                                   message=f"Application {application_id} DELETED")

    def run(self):
        self.start_listener()

    def start_listener(self):
        while self.source.has_next():
            msg = self.source.receive_message()
            logger.debug(f"Processing message {msg}")
            result = None

            # Process request
            try:
                result = self.process_message(msg)
            except Exception as e:
                print(e)
                self.source.socket.send(
                    json.dumps(
                        ErrorApplicationOperationResponse(app_id="", message=str(e)), cls=DataClassJSONEncoder
                    ).encode('utf-8')
                )

            # Send reply
            try:
                self.source.socket.send(json.dumps(result, cls=DataClassJSONEncoder).encode('utf-8'))
            except Exception as e:
                self.source.socket.send(
                    msg=json.dumps(
                        ErrorApplicationOperationResponse(result.app_id, str(e)), cls=DataClassJSONEncoder
                    ).encode('utf-8')
                )

    def process_message(self, msg: dict) -> ApplicationOperationResponse:
        try:
            app_settings = AppConfig(
                application_id=msg.get("id"),
                application_name=msg.get("name"),
                private_key=msg.get("userKey"),
                action=msg.get("action")
            )
        except Exception as e:
            return ErrorApplicationOperationResponse(app_id="", message=str(e), status=HTTPStatus.BAD_REQUEST)
        if app_settings.action.upper() == "CREATE":
            return self.create_application(app_settings)
        elif app_settings.action.upper() == 'DELETE':
            return self.delete_application(app_settings.application_id)
        else:
            return ErrorApplicationOperationResponse(
                app_id=app_settings.application_id,
                message='Invalid application status. Application status needs to be one of ["CREATE", "DELETE"])',
                status=HTTPStatus.BAD_REQUEST
            )

    def setup(self):
        self.source.connect()

    def _sync_apps(self):
        logger.debug("Syncing apps.")
        running_apps = list(self.active_apps.keys())
        db_apps = self.db.read_apps()
        db_app_ids = [app['application_id'] for app in db_apps]
        # check if all apps in db are ready
        for app in db_apps:
            if app['application_status'] == 'READY':
                if app['application_id'] not in running_apps:
                    self.create_application(AppConfig(**app))
        # check if any local apps are running that do not exist in db
        for app_id in running_apps:
            if app_id not in db_app_ids:
                self.delete_application(app_id)
        self.sync_timer.reset_timer()


def start_process(app: Application):
    logger.debug(f'Starting application {app}.')
    app.start()
    logger.debug(f"Application {app} Started.")
