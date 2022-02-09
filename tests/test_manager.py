import logging
import unittest
import uuid

from logsight_classes.data_class import AppConfig
from logsight_classes.responses import SuccessResponse, ErrorResponse
from run import create_manager
from services.configurator import ManagerConfig

logger = logging.getLogger("logsight." + __name__)


class TestManager(unittest.TestCase):

    def setUp(self) -> None:
        super().setUp()
        connection_conf_path = 'config/connections.json'
        manager_conf_path = "config/manager.json"
        config = ManagerConfig(connection_conf_path, manager_conf_path)
        self.manager = create_manager(config)
        logger.info("Manager created successfully.")

    def test_create_application_pass(self):
        logger.info("Running test_create_application_pass")
        app_id_1 = uuid.uuid4()
        app_settings = AppConfig(application_id=app_id_1,
                                  application_name="myapplication",
                                  private_key="myuserkey",
                                  action='')

        result = self.manager.create_application(app_settings)
        assert isinstance(result, SuccessResponse)
        logger.info(f"The result of the creation of application {app_settings} is correct.")
        assert app_settings.application_id in self.manager.active_apps
        assert app_settings.application_id in self.manager.active_process_apps
        logger.info(f"The side effects of creating the application {app_settings} are correct.")
        self.manager.active_process_apps[app_settings.application_id].terminate()
        logger.info(f"Application is terminated.")

    def test_create_application_already_exists_fail(self):
        logger.info("Running test_create_application_fail where application already exists.")
        app_id = uuid.uuid4()
        app_settings = [AppConfig(application_id=app_id,
                                  application_name="myapplication",
                                  private_key="myuserkey",
                                  action=''), AppConfig(application_id=app_id,
                                                        application_name="myapplication",
                                                        private_key="myuserkey",
                                                        action='')]
        logger.info(f"Creating application configs for {len(app_settings)} applications: ")
        logger.info(f"Running the create_application method for the  {len(app_settings)} applications.")
        result = self.manager.create_application(app_settings[0])
        logger.info(f"The result of the creation of application the first app is correct.")
        assert isinstance(result, SuccessResponse)
        assert app_settings[0].application_id in self.manager.active_apps
        assert app_settings[0].application_id in self.manager.active_process_apps
        logger.info(f"The side effects of creating the first application are correct.")
        self.manager.active_process_apps[app_settings[0].application_id].terminate()
        logger.info(f"Application is terminated.")
        result = self.manager.create_application(app_settings[1])
        assert isinstance(result, SuccessResponse)
        logger.info(f"The result of the creation of application the first app is correct.")
        self.manager.active_process_apps[app_settings[1].application_id].terminate()
        logger.info(f"Application is terminated.")

    def test_delete_application_pass(self):
        # creating application given
        app_id = str(uuid.uuid4())
        app_settings = AppConfig(application_id=app_id,
                                 application_name="myapp",
                                 private_key="myprivatekey",
                                 action="CREATE")
        result = self.manager.create_application(app_settings)
        assert isinstance(result, SuccessResponse)
        # deleting application
        result = self.manager.delete_application(app_settings.application_id)
        assert isinstance(result, SuccessResponse)
        assert app_settings.application_id not in self.manager.active_apps
        assert app_settings.application_id not in self.manager.active_process_apps

    def test_delete_application_fail(self):
        # creating application given
        app_id = str(uuid.uuid4())
        app_settings = AppConfig(application_id=app_id,
                                 application_name="myapp",
                                 private_key="myprivatekey",
                                 action="CREATE")
        result = self.manager.create_application(app_settings)
        assert isinstance(result, SuccessResponse)
        # deleting application
        result = self.manager.delete_application(uuid.uuid4())  # not existing application id
        assert isinstance(result, SuccessResponse)
        self.manager.active_process_apps[uuid.UUID(app_id)].terminate()

    def test_process_message_pass(self):
        app_id = str(uuid.uuid4())
        msg = {"id": app_id, "name": "myapp", "userKey": "myprivatekey", "action": "CREATE"}
        result = self.manager.process_message(msg)
        assert isinstance(result, SuccessResponse)

        msg = {"id": app_id, "name": "myapp", "userKey": "myprivatekey", "action": "DELETE"}
        result = self.manager.process_message(msg)
        assert isinstance(result, SuccessResponse)

        msg = {"id": app_id, "name": "myapp", "userKey": "myprivatekey", "action": ""}
        result = self.manager.process_message(msg)
        assert isinstance(result, ErrorResponse)

    def test_process_message_fail(self):
        app_id = str(uuid.uuid4())
        msg = {"id": app_id, "name": "myapp", "userKey": "myprivatekey", "action": ""}
        result = self.manager.process_message(msg)
        assert Exception

        msg = {"id": "myuuid", "name": "myapp", "userKey": "myprivatekey", "action": "CREATE"}
        result = self.manager.process_message(msg)
        assert isinstance(result, ErrorResponse)

        msg = {"id": str(uuid.uuid4()), "name": None, "userKey": "myprivatekey", "action": "CREATE"}
        result = self.manager.process_message(msg)
        assert isinstance(result, ErrorResponse)

        msg = {"id": str(uuid.uuid4()), "name": "myapp", "action": "CREATE"}
        result = self.manager.process_message(msg)
        assert isinstance(result, ErrorResponse)

        msg = {"id": str(uuid.uuid4()), "name": "", "userKey": "", "action": ""}
        result = self.manager.process_message(msg)
        assert isinstance(result, ErrorResponse)


if __name__ == "__main__":
    unittest.main()
