import os

from config import Config
from dacite import from_dict

from configs.global_vars import CONNECTIONS_PATH, DEBUG, PIPELINE_PATH
from common.logsight_classes.configs import PipelineConfig


class ConnectionConfigParser:
    def __init__(self, connection_config_path: str = CONNECTIONS_PATH):
        self.conns = Config(connection_config_path)

        for conn in self.conns.as_dict():
            for key in self.conns[conn].keys():
                if 'host' in key and DEBUG:
                    self.conns[conn][key] = "localhost"
                if 'url' in key and DEBUG:
                    self.conns[conn][key] = "localhost"

        if "kafka" in self.conns:
            self.conns['kafka']['address'] = f"{self.conns['kafka']['host']}:{self.conns['kafka']['port']}"

    def get_kafka_params(self):
        return self.conns['kafka']

    def get_elasticsearch_params(self):
        return self.conns['elasticsearch']

    def get_connection(self, conn):
        return self.conns.get(conn, {})


class ManagerConfig(ConnectionConfigParser):
    def __init__(self, connection_config_path: str, manager_config_path: str):
        super().__init__(connection_config_path)
        self.manager_config = Config(manager_config_path)

    def get_source(self):
        return self.manager_config['connectors']['source']

    def list_services(self):
        return list(self.manager_config['services'].keys())

    def get_service(self, service):
        return self.manager_config['services'][service]

    def get_producer(self):
        return self.manager_config['connectors']['producer']

    def get_connector(self, connector):
        return self.manager_config['connectors'][connector]

    def get_topic_list(self):
        return self.manager_config.get('topic_list', None)


class ModulePipelineConfig:
    def __init__(self, pipeline_config_path: str = PIPELINE_PATH):
        self.pipeline_config = from_dict(data=Config(pipeline_config_path).as_dict(), data_class=PipelineConfig)
        self._modify_config()

    def __repr__(self):
        return self.pipeline_config

    def _modify_config(self):
        ad = os.environ.get("DISABLE_AD")
        if ad and ad.lower() == "true":
            self.pipeline_config.modules['ad_fork'].next_module.remove('ad_sink')
            del self.pipeline_config.modules['ad_sink']

    def get_module(self, module):
        return self.pipeline_config.modules[module]
