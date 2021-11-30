import logging
import threading
from connectors.sinks import Sink
from connectors.sources import Source
from modules.core import StatefulModule
from modules.core.wrappers import synchronized
from logsight_lib.count_ad import CountADPredictor


class EnumState:
    IDLE = 0
    MODEL_LOADED = 1
    MOVE_STATE = 2


class CountADModule(StatefulModule):
    def __init__(self, data_source: Source, data_sink: Sink, internal_source: Source, internal_sink: Sink, **kwargs):
        super().__init__(data_source, data_sink, internal_source, internal_sink, **kwargs)
        self.state = EnumState.IDLE
        self.module_name = "count_ad"
        self.ad = CountADPredictor()

    def run(self):
        self.start_internal_listener()

    def process_internal_message(self, config):
        if config['type'] == "load":
            self._load_model(config)

    def _load_model(self, config):
        try:
            self.ad.load_model(config)
        except Exception as e:
            logging.log(logging.ERROR, e)
        self._set_loaded_state()

    def _set_loaded_state(self):
        self.state = EnumState.MODEL_LOADED
        threading.Thread(target=self.start_data_stream).start()

    @synchronized
    def process_input(self, input_data):
        return self.ad.predict(input_data)
