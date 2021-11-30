import json
import threading
import logging
from connectors.sinks import Sink
from connectors.sources import Source
from modules.core import StatefulModule
from logsight_lib.anomaly_detection.log_anomaly_detection import LogAnomalyDetector
from modules.core.wrappers import synchronized

logger = logging.getLogger("logsight." + __name__)


class EnumState:
    IDLE = 0
    MODEL_LOADED = 1
    MOVE_STATE = 2


class AnomalyDetectionModule(StatefulModule):
    def __init__(self, data_source: Source, data_sink: Sink, internal_source: Source, internal_sink: Sink,
                 config):
        super().__init__(data_source, data_sink, internal_source, internal_sink)
        self.module_name = "anomaly_detection"
        self.state = EnumState.IDLE

        self.buffer = []
        self.buffer_size = config.buffer_size
        self.config = config
        self.ad = LogAnomalyDetector()
        self.timeout_period = self.config.timeout_period
        self.timer = threading.Timer(self.timeout_period, self._timeout_call)

    def run(self):
        self._load_model(None, None)
        super(StatefulModule, self).run()

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
        self.state = EnumState.MODEL_LOADED
        logger.debug(f"Creating data source thread for module {self.module_name}.")
        stream = threading.Thread(name=self.module_name + "DatSrc", target=self.start_data_stream, daemon=True)
        stream.start()
        self.timer.start()

    @synchronized
    def process_input(self, input_data):
        self.buffer.append(input_data)
        if len(self.buffer) == self.buffer_size:
            return self._process_buffer()

    @synchronized
    def _process_buffer(self):
        if len(self.buffer) == 0:
            return
        buff_copy = self.buffer.copy()
        result = self.ad.process_log(buff_copy)

        return result

    def _timeout_call(self):
        logger.debug("Initiating timer LogAD")
        result = self._process_buffer()
        self.buffer = []
        self._reset_timer()
        if result:
            try:
                self.data_sink.send(result)
            except Exception as e:
                logger.error(f'{e}')
    def _reset_timer(self):
        self.timer.cancel()
        self.timer = threading.Timer(self.timeout_period, self._timeout_call)
        self.timer.start()
