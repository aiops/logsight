from builders.base import Builder
from builders.connection_builder import ConnectionBuilder

from modules import *


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


class ModuleBuilder(Builder):
    def __init__(self):
        self.sink_builder = ConnectionBuilder()

    def build_object(self, object_config, app_settings):
        args = object_config['args']
        if "sink" in args.keys():
            sink = self.sink_builder.build_object(args['sink'], app_settings)
            return eval(object_config['classname'])(sink)
        else:
            return eval(object_config['classname'])(Struct(**args))
