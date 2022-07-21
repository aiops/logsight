import json

from config import Config
from dacite import from_dict

import configs.global_vars
from configs.global_vars import CONNECTIONS_PATH, DEBUG, PIPELINE_PATH, LOGS_CONFIG_PATH, FILTER_NORMAL, \
    PIPELINE_CONNECTION


class LogConfig:
    def __init__(self, log_config_path: str = LOGS_CONFIG_PATH, debug=configs.global_vars.DEBUG):
        self.config = json.load(open(log_config_path, 'r'))
        if debug:
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
        if FILTER_NORMAL is True:
            filter_normal_config = ModuleConfig(classname="FilterModule", args={
                "key": "metadata.prediction",
                "condition": "equals",
                "value": 1
            }, next_module="risk_score")
            self.pipeline_config.modules['filter_normal'] = filter_normal_config
            self.pipeline_config.modules['log_ad'].next_module = "filter_normal"

        self.pipeline_config.connectors.data_source.connector.connection = PIPELINE_CONNECTION

    def get_module(self, module):
        return self.pipeline_config.modules[module]
