import json
from pathlib import Path
from config.globals import DEBUG, CONFIG_PATH


class Config:
    def __init__(self, path=Path(CONFIG_PATH) / "connections.json"):
        self.conns = json.load(open(path, 'r'))

        for conn in self.conns:
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


class ManagerConfig(Config):
    def __init__(self):
        super().__init__()
        self.manager_config = json.load(open(Path(CONFIG_PATH) / "manager.json", 'r'))

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
        return self.manager_config['topic_list']


class ModuleConfig(Config):
    def __init__(self):
        super().__init__()
        self.module_config = json.load(open(Path(CONFIG_PATH) / "modules.json", 'r'))

    def get_module(self, module):
        return self.module_config[module]

    def get_connector(self, module, connector):
        return self.module_config[module][connector]
