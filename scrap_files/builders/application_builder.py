import logging
from typing import Dict, Optional

from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from common.logsight_classes.configs import AppConfig, ModuleConfig, PipelineConfig
from common.patterns.builder import Builder
from common.patterns.chain_of_responsibility import AbstractHandler
from configs.global_vars import USES_ES, USES_KAFKA
from pipeline.builders.module_builder import ModuleBuilder
from scrap_files.application import Application

logger = logging.getLogger("logsight." + __name__)


class ApplicationBuilder(Builder):
    def __init__(self, services, module_builder: ModuleBuilder = None):
        self.kafka_admin = services.get('kafka_admin', None) if USES_KAFKA else None
        self.es_admin = services.get('elasticsearch_admin', None) if USES_ES else None
        self.module_builder = module_builder if module_builder else ModuleBuilder()

    def build(self, object_config: PipelineConfig) -> Application:
        if self.es_admin:
            self.es_admin.create_indices(app_config.private_key, app_config.application_name)

        kafka_topics = self._create_kafka_topics(pipeline_config, app_config)

        start_module = pipeline_config.metadata.input
        handler_objects = {
            name: self.module_builder.build(obj)
            for name, obj in pipeline_config.handlers.items()
        }

        self._connect_handler(handler_objects, pipeline_config.handlers, start_module)
        return Application(handlers=handler_objects, input_module=handler_objects[start_module], **app_config.dict(),
                           topic_list=kafka_topics)

    def _connect_handler(self, handlers: Dict[str, AbstractHandler], handlers_dict: Dict[str, ModuleConfig],
                         cur_handler: str):

        if cur_handler == "":
            return
        start_handler = handlers[cur_handler]
        next_handler_name = handlers_dict[cur_handler].next_module
        if isinstance(next_handler_name, list):
            for s in next_handler_name:
                next_handler = handlers.get(s)
                self._connect_handler(handlers, handlers_dict, s)
                self._set_next_handler(start_handler, next_handler)
        else:
            self._connect_handler(handlers, handlers_dict, handlers_dict[cur_handler].next_module)
            if handlers_dict[cur_handler].next_module == "":
                return
            next_handler = handlers.get(handlers_dict[cur_handler].next_module)

            self._set_next_handler(start_handler, next_handler)

    @staticmethod
    def _set_next_handler(handler, next_handler: Optional[ModuleConfig]):
        if next_handler:
            return handler.set_next(next_handler)

    def _create_kafka_topics(self, pipeline_config: PipelineConfig, app_config: AppConfig):
        created_topics = []
        if self.kafka_admin:
            topic_prefix = "_".join([app_config.private_key, app_config.application_name])
            for topic in pipeline_config.metadata.kafka_topics:
                topic_name = "_".join([topic_prefix, topic])
                try:
                    self.kafka_admin.create_topics([
                        NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
                    logger.debug(f"Created topic {topic}")
                except TopicAlreadyExistsError:
                    logger.error(f"Topic already exists with topic name {topic_name}.")
                created_topics.append(topic_name)
        return created_topics
