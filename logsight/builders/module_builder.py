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
        if {"source", "sink"}.intersection(set(args.keys())):
            conn = self.conn_builder.build_object(args.get('source', args.get('sink')), app_settings)
            return c_name(conn)
        else:
            return c_name(Struct(**args),app_settings=app_settings)
