import logging
import sys
from copy import deepcopy
from time import time

from kafka.admin import NewTopic

from services.configurator import ModuleConfig
from modules import *
from connectors.sources import *
from connectors.sinks import *

logger = logging.getLogger("logsight." + __name__)

module_classes = {"log_parsing": LogParserModule, "model_training": ModelTrainModule,
                  "anomaly_detection": AnomalyDetectionModule, "incidents": LogIncidentModule,
                  "field_parser": FieldParsingModule, "log_aggregation": LogAggregationModule}


class Application:
    def __init__(self, application_id, private_key, application_name, kafka_topic_list, modules,
                 input_module, services=None,
                 **kwargs):
        self.application_id = str(application_id)
        self.application_name = application_name
        self.private_key = private_key
        self.modules = modules
        self.services = services or []
        self.input_module = input_module
        self.topic_list = kafka_topic_list

    def start(self):
        for module_name, module in self.modules.items():
            t = time()
            logger.debug(f"Initializing module {module_name}")
            module.connect()
            logger.debug('Module connected')
            module.run()
            logger.debug(f"Module {module_name} initialized in {time() - t}s")

    def __repr__(self):
        return "-".join([self.application_id, self.application_name])

    def to_json(self):
        return {
            "application_id": self.application_id,
            "application_name": self.application_name,
            "input": self.input_module.to_json()
        }


class AppBuilder:
    def __init__(self, kafka_admin=None, es_admin=None):
        self.kafka_admin = kafka_admin
        self.es_admin = es_admin
        self.kafka_topic_list = []
        self.module_config = ModuleConfig()

    def build_app(self, app_settings):
        modules = ['field_parser', 'log_parsing', 'anomaly_detection', 'log_aggregation', 'incidents']
        INPUT_MODULE = "field_parser"
        if self.es_admin:
            self.es_admin.create_indices(app_settings['private_key'], app_settings['application_name'])
        return None
        # module_objects = [(m, self.module_config.get_module(m)) for m in modules]
        # module_objects = {m_name: self._build_module(m_name, obj, app_settings) for m_name, obj in module_objects}
        #
        # self._connect_queues(module_objects)
        # if self.kafka_admin:
        #     app_settings['kafka_topic_list'] = self.kafka_topic_list
        # return Application(**app_settings, modules=module_objects, input_module=module_objects[INPUT_MODULE])

    def _connect_queues(self, module_objects):
        for module_name, module in module_objects.items():
            if module.has_internal_queue_src:
                self._connect_internal_queue(module, module_objects[module.control_source.link].control_sink.queue)
            print(module.module_name, module.has_data_queue_src)
            if module.has_data_queue_src:
                self._connect_data_queue(module, module_objects[module.data_source.link].data_sink)

    @staticmethod
    def _connect_internal_queue(module, tgt):
        module.control_source.connect(tgt)

    @staticmethod
    def _connect_data_queue(module, tgt_sink):
        if isinstance(tgt_sink, MultiSink):
            for sink in tgt_sink.sinks:
                if hasattr(sink, "queue"):
                    module.data_source.connect(sink.queue)
        else:
            module.data_source.connect(tgt_sink.queue)

    def _build_module(self, module_name, module, app_settings):
        data_source = self.setup_connector(module_name, 'data_source', self.module_config, app_settings)
        data_sink = self.setup_connector(module_name, 'data_sink', self.module_config, app_settings)
        internal_source = self.setup_connector(module_name, 'control_source', self.module_config, app_settings)
        internal_sink = self.setup_connector(module_name, 'control_sink', self.module_config, app_settings)
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

    def setup_connector(self, module, connector, config, app_settings):
        conn_config = config.get_connector(module, connector)
        if len(conn_config) == 0:
            return
        if "multi" in conn_config['connection']:
            return self._build_multi_connection(module, connector, config, app_settings)
        return self._build_single_connection(conn_config, config, app_settings)

    def _build_single_connection(self, conn_config, config, app_settings):
        if len(conn_config) == 0:
            return
        conn_params = deepcopy(config.get_connection(conn_config['connection']))
        conn_params.update(conn_config['params'])
        conn_params.update(app_settings)
        private_key = app_settings['private_key']
        app_name = app_settings['application_name']

        if conn_config['connection'] == "kafka":
            self._create_kafka_topic(conn_params['topic'], private_key, app_name)

        return eval(conn_config['classname'])(**conn_params)

    def _create_kafka_topic(self, topic, private_key, app_name):
        topic = "_".join([private_key, app_name, topic])
        if self.kafka_admin:
            try:
                self.kafka_admin.create_topics([
                    NewTopic(name=topic, num_partitions=1, replication_factor=1)])
                logger.debug(f"Created topic {topic}")
                self.kafka_topic_list.append(topic)
            except Exception:
                logger.error(f"Topic already exists with topic name {topic}.")
        else:
            logger.error("Kafka admin not initialized.")

    def _build_multi_connection(self, module, connector, config, app_settings):
        conn_config = config.get_connector(module, connector)
        connectors = []
        for conn_type in conn_config["params"]:
            for conn in conn_config["params"][conn_type]:
                connectors.append(self._build_single_connection(conn, config, app_settings))
        conn = "sinks" if "sink" in connector else "sources"
        conn_params = {conn: connectors}
        return eval(conn_config['classname'])(**conn_params)


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)
