from dacite import from_dict

from common.logsight_classes.configs import AdapterConfigProperties, ModuleConfig
from common.patterns.builder import Builder
from connectors.builders.adapter_builder import AdapterBuilder
from pipeline import modules
from pipeline.modules.core import ConnectableModule, Module


class ModuleBuilder(Builder):
    """
    Builder class for building Modules.
    """

    def __init__(self, connection_builder: AdapterBuilder = None):
        self.conn_builder = connection_builder if connection_builder else AdapterBuilder()

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
                from_dict(data=config.args['connector'], data_class=AdapterConfigProperties))
        return c_name(**args)
