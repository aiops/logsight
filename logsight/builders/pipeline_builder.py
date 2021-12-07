from builders.base import Builder
from builders.module_builder import ModuleBuilder
from logsight_classes.pipeline import Pipeline


class PipelineBuilder(Builder):
    def __init__(self):
        self.module_builder = ModuleBuilder()

    def build_object(self, pipeline_config: dict, **kwargs):
        handler_objects = {}
        start_module = pipeline_config['metadata']['start']
        for name, obj in pipeline_config['handlers'].items():
            handler_objects[name] = self.module_builder.build_object(obj, pipeline_config['metadata']['app_settings'])

        self._connect_handler(handler_objects, pipeline_config['handlers'], start_module)

        return Pipeline(handlers=handler_objects, start=start_module)

    def _connect_handler(self, handlers, handlers_dict, start):
        if start == "":
            return
        start_handler = handlers[start]
        next_handler_name = handlers_dict[start]['next_handler']
        if isinstance(next_handler_name, list):
            for s in next_handler_name:
                next_handler = handlers[s]
                self._connect_handler(handlers, handlers_dict, s)
                self._set_next_handler(start_handler, next_handler)
        else:
            self._connect_handler(handlers, handlers_dict, handlers_dict[start]['next_handler'])
            if handlers_dict[start]['next_handler'] == "":
                return
            next_handler = handlers[handlers_dict[start]['next_handler']]

            self._set_next_handler(start_handler, next_handler)

    @staticmethod
    def _set_next_handler(handler, next_handler=""):
        if next_handler == "":
            return
        return handler.set_next(next_handler)
