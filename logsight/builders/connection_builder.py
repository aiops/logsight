from copy import deepcopy

from builders.base import Builder
from services import Config

from connectors.sinks import *


class ConnectionBuilder(Builder):
    def __init__(self):
        self.conn_config = Config()

    def build_object(self, object_config, app_settings):
        conn_params = deepcopy(self.conn_config.get_connection(object_config['connection']))
        conn_params.update(object_config['params'])
        conn_params.update(app_settings)
        return eval(object_config['classname'])(**conn_params)
