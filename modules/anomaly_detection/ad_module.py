import json
import threading
import logging
from connectors.sink import Sink
from connectors.source import Source
from modules.api import StatefulModule
from modules.anomaly_detection.log_anomaly_detection import LogAnomalyDetector
from modules.api.wrappers import synchronized
from modules.api.enum import State


class AnomalyDetectionModule(StatefulModule):
    def __init__(self, data_source: Source, data_sink: Sink, internal_source: Source, internal_sink: Sink,
                 config):
        super().__init__(data_source, data_sink, internal_source, internal_sink)
        self.module_name = "anomaly_detection"
        self.state = State.IDLE

        self.buffer = []
        self.buffer_size = config.buffer_size
        self.config = config
        self.ad = LogAnomalyDetector()
        self.timeout_period = self.config.timeout_period
        self.timer = threading.Timer(self.timeout_period, self._timeout_call)

    def run(self):
        self.internal_source.connect()
        internal = threading.Thread(target=self.start_internal_listener)
        internal.start()
        self._load_model(None, None)

    def process_internal_message(self, msg):
        if msg['type'] == "load":
            self._load_model(None, None)

    def _load_model(self, version, app_name):
        try:
            self.ad.load_model(version, app_name)
        except Exception as e:
            logging.log(logging.ERROR, e)
        self._set_loaded_state()

    def _set_loaded_state(self):
        self.state = State.MODEL_LOADED
        self.timer.start()
        print("AD MODULE LOADED STATE")
        data_stream = threading.Thread(target=self.start_data_stream)
        data_stream.start()

    @synchronized
    def process_input(self, input_data):
        self.buffer.append(input_data)
        if len(self.buffer) == self.buffer_size:
            print("Processing buffer")
            return self._process_buffer()
        else:
            print(f"Buffering {input_data}")

    @synchronized
    def _process_buffer(self):
        if len(self.buffer) == 0:
            return
        result = self.ad.process_log(self.buffer)
        self.buffer = []
        return result

    def _timeout_call(self):
        print("Initiating timer")
        result = self._process_buffer()
        self.buffer = []
        try:
            self.data_sink.send(result)
        except Exception as e:
            print(f'{e}')
