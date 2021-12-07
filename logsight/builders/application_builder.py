import logging
from typing import Dict, Optional

from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from builders.base import Builder
from builders.module_builder import ModuleBuilder
from logsight_classes.application import Application
from logsight_classes.data_class import AppConfig, PipelineConfig, HandlerConfig
from modules.core import AbstractHandler

logger = logging.getLogger("logsight." + __name__)


class ApplicationBuilder(Builder):
    def __init__(self, kafka_admin=None, es_admin=None):
        self.kafka_admin = kafka_admin
        self.es_admin = es_admin
        self.module_builder = ModuleBuilder()

    def build_object(self, pipeline_config: PipelineConfig, app_config: AppConfig) -> Application:
        if self.es_admin:
            self.es_admin.create_indices(app_config.private_key, app_config.application_name)

        kafka_topics = self._create_kafka_topics(pipeline_config, app_config)

        start_module = pipeline_config.metadata.input
        handler_objects = {
            name: self.module_builder.build_object(obj, app_config)
            for name, obj in pipeline_config.handlers.items()
        }

        self._connect_handler(handler_objects, pipeline_config.handlers, start_module)
        return Application(handlers=handler_objects, input_module=handler_objects[start_module], **app_config.dict(),
                           topic_list=kafka_topics)

    def _connect_handler(self, handlers: Dict[str, AbstractHandler], handlers_dict: Dict[str, HandlerConfig],
                         cur_handler: str):
        if cur_handler == "":
            return
        start_handler = handlers[cur_handler]
        next_handler_name = handlers_dict[cur_handler].next_handler
        if isinstance(next_handler_name, list):
            for s in next_handler_name:
                next_handler = handlers.get(s)
                self._connect_handler(handlers, handlers_dict, s)
                self._set_next_handler(start_handler, next_handler)
        else:
            self._connect_handler(handlers, handlers_dict, handlers_dict[cur_handler].next_handler)
            if handlers_dict[cur_handler].next_handler == "":
                return
            next_handler = handlers.get(handlers_dict[cur_handler].next_handler)

            self._set_next_handler(start_handler, next_handler)

    @staticmethod
    def _set_next_handler(handler, next_handler: Optional[HandlerConfig]):
        if next_handler:
            return handler.set_next(next_handler)

    def _create_kafka_topics(self, pipeline_config: PipelineConfig, app_config: AppConfig):
        created_topics = []
        if self.kafka_admin:
            topic_prefix = "_".join([app_config.private_key, app_config.application_name])
            for topic in pipeline_config.metadata.kafka_topics:
                try:
                    self.kafka_admin.create_topics([
                        NewTopic(name="_".join([topic_prefix, topic]), num_partitions=1, replication_factor=1)])
                    logger.debug(f"Created topic {topic}")
                    created_topics.append(topic)
                except TopicAlreadyExistsError:
                    logger.error(f"Topic already exists with topic name {topic}.")
            logger.error("Kafka admin not initialized.")
        return created_topics
