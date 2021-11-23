import logging
import sys
from copy import deepcopy
from time import time

from kafka.admin import NewTopic

from services.configurator import ModuleConfig
from modules import *
from connectors.source import *
from connectors.sink import *

from multiprocessing import Process

logger = logging.getLogger("logsight." + __name__)


class Application:
    def __init__(self, application_id, private_key, user_name, application_name, modules, input_module, services=None,
                 topic_list=None, **kwargs):
        self.application_id = application_id
        self.application_name = application_name
        self.user_name = user_name
        self.private_key = private_key
        self.modules = modules
        self.services = services or []
        self.topic_list = topic_list or []
        self.input_module = input_module

    def start(self):
        for module_name, module in self.modules.items():
            t = time()
            logger.debug(f"Initializing module {module_name}")
            module.connect()
            module.run()
            logger.debug(f"Module initialized in {time()-t}s")

    def __repr__(self):
        return "-".join([self.application_id, self.application_name])

    def to_json(self):
        return {
            "application_id": self.application_id,
            "application_name": self.application_name,
            "user_name": self.user_name,
            "topic_list": self.topic_list,
            "input": self.input_module.to_json()
        }


module_classes = {"log_parsing": ParserModule, "model_training": ModelTrainModule,
                  "anomaly_detection": AnomalyDetectionModule, "incidents": LogIncidentModule,
                  "field_parser": FieldParserModule, "log_aggregation": LogAggregationModule}


class AppBuilder:
    def __init__(self, kafka_admin=None, es_admin=None):
        self.kafka_admin = kafka_admin
        self.es_admin = es_admin
        self.module_config = ModuleConfig()

    def build_app(self, app_settings, modules='all'):
        modules = ['field_parser', 'log_parsing', 'anomaly_detection', 'log_aggregation']
        # modules = ['log_parsing', 'anomaly_detection', 'incidents']

        # modules = ['anomaly_detection']
        INPUT_MODULE = "field_parser"
        created_topics = None
        if self.kafka_admin:
            created_topics = self._prepare_kafka_topics(modules, app_settings['private_key'],
                                                        app_settings['application_name'])

        if self.es_admin:
            self.es_admin.create_indices(app_settings['private_key'], app_settings['application_name'])

        module_objects = [(m, self.module_config.get_module(m)) for m in modules]
        module_objects = {m_name: self._build_module(m_name, obj, app_settings) for m_name, obj in module_objects}

        for module_name, module in module_objects.items():
            if module.has_internal_queue_src:
                module.internal_source.connect(module_objects[module.internal_source.link].internal_sink.queue)
            if module.has_data_queue_src:
                tgt_sink = module_objects[module.data_source.link].data_sink
                _queue = None
                if isinstance(tgt_sink, MultiSink):
                    for sink in tgt_sink.sinks:
                        _queue = sink.queue if hasattr(sink, "queue") else None
                else:
                    _queue = tgt_sink.queue
                module.data_source.connect(_queue)

        return Application(**app_settings, modules=module_objects, topic_list=created_topics,
                           input_module=module_objects[INPUT_MODULE])

    def _build_module(self, module_name, module, app_settings):
        data_source = setup_connector(module_name, 'data_source', self.module_config, app_settings)
        data_sink = setup_connector(module_name, 'data_sink', self.module_config, app_settings)
        internal_source = setup_connector(module_name, 'internal_source', self.module_config, app_settings)
        internal_sink = setup_connector(module_name, 'internal_sink', self.module_config, app_settings)
        config = module.get('configs', {})
        config.update(app_settings)
        config = Struct(**config)

        return module_classes[module_name](
            data_sink=data_sink,
            data_source=data_source,
            internal_sink=internal_sink,
            internal_source=internal_source,
            config=config,
        )

    def _prepare_kafka_topics(self, modules, private_key, app_name):
        topic_list = ["_".join([private_key, app_name, module]) for module in modules]
        topic_list.extend(["_".join([private_key, app_name, module, 'internal']) for module in modules])
        for topic in topic_list:
            try:
                self.kafka_admin.create_topics([
                    NewTopic(name=topic, num_partitions=1, replication_factor=1)])
                logger.debug(f"Created topic {topic}")
            except Exception as e:
                logger.error(f"Topic already exists with topic name {topic}.")
        return topic_list


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


def setup_connector(module, connector, config, app_settings):
    conn_config = config.get_connector(module, connector)
    if len(conn_config) == 0:
        return
    if "multi" in conn_config['connection']:
        return build_multi_connection(module, connector, config, app_settings)
    return _build_single_connection(conn_config, config, app_settings)


def _build_single_connection(conn_config, config, app_settings):
    if len(conn_config) == 0:
        return
    conn_params = deepcopy(config.get_connection(conn_config['connection']))
    conn_params.update(conn_config['params'])
    conn_params.update(app_settings)
    return eval(conn_config['classname'])(**conn_params)


def build_multi_connection(module, connector, config, app_settings):
    conn_config = config.get_connector(module, connector)
    connectors = []
    for conn_type in conn_config["params"]:
        for conn in conn_config["params"][conn_type]:
            connectors.append(_build_single_connection(conn, config, app_settings))
    conn = "sinks" if "sink" in connector else "sources"
    conn_params = {conn: connectors}
    return eval(conn_config['classname'])(**conn_params)
