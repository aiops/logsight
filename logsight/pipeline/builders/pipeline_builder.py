from typing import Dict, Tuple

from common.logsight_classes.configs import ModuleConfig, PipelineConfig, PipelineConnectors
from common.patterns.builder import Builder, BuilderException
from connectors import Source
from connectors.connection_builder import ConnectionBuilder
from connectors.sources.source import ConnectableSource
from pipeline.builders.module_builder import ModuleBuilder
from pipeline.modules.core import Module
from pipeline.pipeline import Pipeline


class PipelineBuilder(Builder):
    """
    Builder class for building pipelines.
    """

    def __init__(self):
        self.module_builder = ModuleBuilder()
        self.connection_builder = ConnectionBuilder()

    def build(self, pipeline_config: PipelineConfig) -> Pipeline:
        """
        It takes a pipeline configuration and returns a pipeline object
        Args:
            pipeline_config: (PipelineConfig): Pipeline configuration object
        Returns:
            Pipeline: A pipeline object.
        """
        module_objects = self._build_modules(pipeline_config.modules)
        input_module_name = self._infer_input_module(pipeline_config.modules)

        data_source, control_source = self._build_connectors(pipeline_config.connectors)
        return Pipeline(modules=module_objects, data_source=data_source, control_source=control_source,
                        input_module=module_objects[input_module_name],
                        metadata=pipeline_config.metadata)

    def _build_connectors(self, connectors: PipelineConnectors) -> Tuple[Source, ConnectableSource]:
        """
        Builds connections for data and control channel of pipeline using given configuration object.

        Args:
            connectors (PipelineConnectors): Configuration object for creating connectors
        Returns:
            Tuple[Source, Source] : Source objects for data and control channels
        """
        data_source: Source = self.connection_builder.build(connectors.data_source)
        control_source = self.connection_builder.build(connectors.control_source)
        return data_source, control_source

    def _build_modules(self, modules_cfg: Dict[str, ModuleConfig]) -> Dict[str, Module]:
        """
        The _build_modules function builds the modules from a dictionary of module configurations.
        The function takes in a dictionary of module configurations and returns a dictionary of built modules.
        Args:
            modules_cfg (Dict[str, ModuleConfig]): Dictionary of Module configurations
        Returns:
            dict: A dictionary of module objects
        """

        module_objects = dict()
        for name, cfg in modules_cfg.items():
            obj = self.module_builder.build(cfg)
            obj.name = name
            module_objects[name] = obj

        self._connect_modules(module_objects, modules_cfg)
        return module_objects

    @staticmethod
    def _infer_input_module(modules_cfg: Dict[str, ModuleConfig]) -> str:
        """
        The _infer_input_module function is used to infer the input module for the pipeline.
        Args:
            modules_cfg (Dict[str, ModuleConfig]): Dictionary of Module configurations
        Returns:
            The name of the module that has no previous modules
        """
        candidates = set()
        has_prev = set()

        def add_and_discard(m_name):
            has_prev.add(m_name)
            if m_name in candidates:
                candidates.discard(m_name)

        for module_name, module in modules_cfg.items():
            if module_name not in has_prev:
                candidates.add(module_name)
            if isinstance(module.next_module, list):
                for m in module.next_module:
                    add_and_discard(m)
            else:
                add_and_discard(module.next_module)
        if len(candidates) == 1:
            return candidates.pop()
        else:
            raise BuilderException(
                f"Cannot infer input module. Only one candidate can be input. Candidates: {candidates}")

    @staticmethod
    def _connect_modules(modules: Dict[str, Module], modules_config: Dict[str, ModuleConfig]) -> Dict[str, Module]:
        """
        Connects the modules by linking them. This is possible using the `AbstractHandler` class.
        Args:
            modules (Dict[str, Module]): Dictionary containing created module objects.
            modules_config (Dict[str, ModuleConfig]): Dictionary containing module configuration objects.

        Returns:
            modules(Dict[str, Module]): Connected modules.
        """
        for module_name, module_obj in modules.items():
            next_handlers = modules_config[module_name].next_module
            # convert to list if only a single handler
            next_handlers = [next_handlers] if not isinstance(next_handlers, list) else next_handlers

            for handler_name in next_handlers:
                if handler_name:
                    next_handler_obj = modules[handler_name]
                    module_obj.set_next(next_handler_obj)
        return modules
