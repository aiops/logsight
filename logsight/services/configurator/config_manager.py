import json

from config import Config
from dacite import from_dict

from common.logsight_classes.configs import PipelineConfig
from configs.global_vars import CONNECTIONS_PATH, DEBUG, PIPELINE_PATH, LOGS_CONFIG_PATH, DEBUG


class LogConfig:
    def __init__(self, log_config_path: str = LOGS_CONFIG_PATH):
        self.config = json.load(open(log_config_path, 'r'))
        if DEBUG:
            self.config['loggers']['logsight']['handlers'] = ["debug", "warning"]
            self.config['loggers']['logsight']['level'] = 'DEBUG'


class ConnectionConfig:
    def __init__(self, connection_config_path: str = CONNECTIONS_PATH):
        self.conns = Config(connection_config_path)

        for conn in self.conns.as_dict():
            for key in self.conns[conn].keys():
                if 'host' in key and DEBUG:
                    self.conns[conn][key] = "localhost"
                if 'url' in key and DEBUG:
                    self.conns[conn][key] = "localhost"

    def get_kafka_params(self):
        return self.conns['kafka']

    def get_elasticsearch_params(self):
        return self.conns['elasticsearch']

    def get_postgres_params(self):
        return self.conns['postgres']

    def get_connection(self, conn):
        return self.conns.get(conn, {})


class ModulePipelineConfig:
    def __init__(self, pipeline_config_path: str = PIPELINE_PATH):
        self.pipeline_config = from_dict(data=Config(pipeline_config_path).as_dict(), data_class=PipelineConfig)

    def __repr__(self):
        return self.pipeline_config

    def get_module(self, module):
        return self.pipeline_config.modules[module]
