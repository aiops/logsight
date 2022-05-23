from dacite import from_dict

from common.logsight_classes.configs import ConnectionConfigProperties, ModuleConfig
from common.patterns.builder import Builder
from connectors.connection_builder import ConnectionBuilder
from pipeline import modules
from pipeline.modules.core import ConnectableModule, Module


class ModuleBuilder(Builder):
    """
    Builder class for building Modules.
    """

    def __init__(self, connection_builder: ConnectionBuilder = None):
        self.conn_builder = connection_builder if connection_builder else ConnectionBuilder()

    def build(self, config: ModuleConfig) -> Module:
        """
          It takes a module configuration and returns a module object.
          Args:
              config: (ModuleConfig): Module  configuration object

          Returns:
            Module: A module object
      """
        args = config.args
        c_name = getattr(modules, config.classname)
        if issubclass(c_name, ConnectableModule):
            config.args['connector'] = self.conn_builder.build(
                from_dict(data=config.args['connector'], data_class=ConnectionConfigProperties))
        return c_name(**args)
