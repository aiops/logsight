import json

from config.global_vars import DEBUG


class ConnectionConfig:
    def __init__(self, connection_config_path: str):
        self.conns = json.load(open(connection_config_path, 'r'))

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


class ManagerConfig(ConnectionConfig):
    def __init__(self, connection_config_path: str, manager_config_path: str):
        super().__init__(connection_config_path)
        self.manager_config = json.load(open(manager_config_path, 'r'))

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


class ModulePipelineConfig(ConnectionConfig):
    def __init__(self, connection_config_path: str, pipeline_config_path: str):
        super().__init__(connection_config_path)
        self.module_pipline_config = json.load(open(pipeline_config_path, 'r'))

    def get_module(self, module):
        return self.module_pipline_config[module]

    def get_connector(self, module, connector):
        return self.module_pipline_config[module][connector]
