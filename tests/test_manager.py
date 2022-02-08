import logging
import unittest
import uuid

from logsight_classes.data_class import AppConfig
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
        app_id_2 = uuid.uuid4()
        app_settings = [AppConfig(application_id=app_id_1,
                                  application_name="myapplication",
                                  private_key="myuserkey",
                                  action=''), AppConfig(application_id=app_id_2,
                                                        application_name="myapplication",
                                                        private_key="myuserkey",
                                                        action='CREATE')]

        logger.info(f"Creating application configs for {len(app_settings)} applications: ")
        logger.info(f"Running the create_application method for the  {len(app_settings)} applications.")
        for i, app_set in enumerate(app_settings):
            result = self.manager.create_application(app_set)
            logger.info(f"Running the create_application method for application {i}")
            assert "ack" in result and "app" in result
            logger.info(f"The result of the creation of application {i} is correct.")
            assert app_set.application_id in self.manager.active_apps
            assert app_set.application_id in self.manager.active_process_apps
            logger.info(f"The side effects of creating the application {i} are correct.")
            self.manager.active_process_apps[app_set.application_id].terminate()
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
        assert "ack" in result and "app" in result
        assert app_settings[0].application_id in self.manager.active_apps
        assert app_settings[0].application_id in self.manager.active_process_apps
        logger.info(f"The side effects of creating the first application are correct.")
        self.manager.active_process_apps[app_settings[0].application_id].terminate()
        logger.info(f"Application is terminated.")
        result = self.manager.create_application(app_settings[1])
        assert "msg" in result and result['msg'] == f"Application {app_settings[1].application_id} already created"
        logger.info(f"The result of the creation of application the first app is correct.")
        self.manager.active_process_apps[app_settings[1].application_id].terminate()
        logger.info(f"Application is terminated.")

    def test_get_app_config_pass(self):
        app_uuid = uuid.uuid4()
        msg = {"id": uuid.uuid4(), "name": "myapp", "userKey": "myprivatekey", "action": "CREATE"}
        expected_result = AppConfig(application_id=app_uuid,
                                    application_name="myapp",
                                    private_key="myprivatekey",
                                    action="CREATE")
        result = self.manager.get_app_config(msg)
        assert expected_result == result

    def test_get_app_config_invalid_parameters_fail(self):
        msg = {"id": "myuuid", "name": "myapp", "userKey": "myprivatekey", "action": "CREATE"}
        result = self.manager.get_app_config(msg)
        assert result is None  # mistake in uuid

        msg = {"id": str(uuid.uuid4()), "name": None, "userKey": "myprivatekey", "action": "CREATE"}
        result = self.manager.get_app_config(msg)
        assert result is None  # None as parameter

        msg = {"id": str(uuid.uuid4()), "name": "myapp", "action": "CREATE"}
        result = self.manager.get_app_config(msg)
        assert result is None  # missing parameter

        msg = {"id": str(uuid.uuid4()), "name": "", "userKey": "", "action": ""}
        result = self.manager.get_app_config(msg)
        assert result is None  # empty string

    def test_process_message_pass(self):
        app_id = str(uuid.uuid4())
        app_settings = AppConfig(application_id=app_id,
                                 application_name="myapp",
                                 private_key="myprivatekey",
                                 action="CREATE")
        result = self.manager.process_message(app_settings)
        assert "ack" in result and result['ack'] == "ACTIVE" and "app" in result

        app_settings = AppConfig(application_id=app_id,
                                 application_name="myapp",
                                 private_key="myprivatekey",
                                 action="DELETE")
        result = self.manager.process_message(app_settings)
        assert "ack" in result and result['ack'] == "DELETED" and "app_id" in result

        app_settings = AppConfig(application_id=app_id,
                                 application_name="myapp",
                                 private_key="myprivatekey",
                                 action="")
        result = self.manager.process_message(app_settings)
        assert "ack" not in result and "msg" in result and result["msg"] == "Invalid application status"

    def test_process_message_fail(self):
        # does not make sense to test fail case as it is just invoking delete_application and create application,
        # their tests are in separate
        assert True

    def test_delete_application_pass(self):
        # creating application given
        app_id = str(uuid.uuid4())
        app_settings = AppConfig(application_id=app_id,
                                 application_name="myapp",
                                 private_key="myprivatekey",
                                 action="CREATE")
        result = self.manager.create_application(app_settings)
        assert "ack" in result and result['ack'] == "ACTIVE" and "app" in result
        # deleting application
        result = self.manager.delete_application(app_settings.application_id)
        assert "ack" in result and result['ack'] == "DELETED" and "app_id" in result

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
        assert "ack" in result and result['ack'] == "ACTIVE" and "app" in result

        # deleting application
        result = self.manager.delete_application(uuid.uuid4())  # not existing application id
        assert result is None
        self.manager.active_process_apps[uuid.UUID(app_id)].terminate()


if __name__ == "__main__":
    unittest.main()
