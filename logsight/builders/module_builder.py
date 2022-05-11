from builders.base import Builder
from builders.connection_builder import ConnectionBuilder
from modules.core import Module
import modules
from logsight_classes.data_class import HandlerConfig, AppConfig


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


class ModuleBuilder(Builder):
    def __init__(self, connection_builder: ConnectionBuilder = None):
        self.conn_builder = connection_builder if connection_builder else ConnectionBuilder()

    def build_object(self, object_config: HandlerConfig, app_settings: AppConfig) -> Module:
        args = object_config.args
        c_name = getattr(modules, object_config.classname)
        if any(substring in string for string in set(args.keys()) for substring in ['source', 'sink']):
            conns = dict()
            for arg in set(args.keys()):
                if any(substring in arg for substring in ['source', 'sink']):
                    conns[arg] = self.conn_builder.build_object(args[arg], app_settings)

            return c_name(**conns)
        else:
            return c_name(Struct(**args), app_settings=app_settings)
