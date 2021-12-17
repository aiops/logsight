from copy import deepcopy
from typing import Union

from builders.base import Builder
from services import ConnectionConfig

import connectors.sinks as sinks
import connectors.sources as sources


class ConnectionBuilder(Builder):
    def __init__(self, config: ConnectionConfig = None):
        self.conn_config = config if config else ConnectionConfig()

    def build_object(self, object_config, app_settings) -> Union[sources.Source, sinks.Sink]:
        conn_params = deepcopy(self.conn_config.get_connection(object_config['connection']))
        conn_params.update(object_config['params'])
        conn_params.update(app_settings.dict())
        try:
            c_name = getattr(sources, object_config['classname'])
        except AttributeError:
            c_name = getattr(sinks, object_config['classname'])
        return c_name(**conn_params)
