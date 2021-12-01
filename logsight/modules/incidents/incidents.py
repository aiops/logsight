import json
import logging
import threading
from datetime import datetime

from connectors.sinks import Sink
from connectors.sources import Source
from modules.core import StatefulModule
from modules.core.wrappers import synchronized
from logsight_lib.incidents import IncidentDetector

logger = logging.getLogger("logsight." + __name__)


class LogIncidentModule(StatefulModule):
    def __init__(self, data_source: Source, data_sink: Sink, internal_source: Source, internal_sink: Sink,
                 config):
        super().__init__(data_source, data_sink, internal_source, internal_sink)
        self.timeout_period = config.timeout_period

        self.model = IncidentDetector()
        self.log_count_buffer = []
        self.log_ad_buffer = []
        self.buffer_size = config.buffer_size
        self.module_name = "incidents"
        self.timer = None

    def process_internal_message(self, msg):
        return

    def run(self):
        super().run()
        self.timer = threading.Timer(self.timeout_period, self._timeout_call)
        self.timer.name = self.module_name+'_timer'
        self.timer.start()

    @synchronized
    def process_input(self, input_data):
        result = None
        if 'timestamp_start' in input_data:
            self.log_count_buffer.append(input_data)
            if float(input_data['prediction']) > 0 or len(input_data['new_templates']) > 0:
                result = self._process_buffer()
                self._reset_state()
        else:
            self.log_ad_buffer.append(input_data)
            timestamp = "@timestamp"
            try:
                end_time = datetime.strptime(self.log_ad_buffer[-1][timestamp], '%Y-%m-%dT%H:%M:%S.%f')
            except Exception:
                end_time = datetime.strptime(self.log_ad_buffer[-1][timestamp], '%Y-%m-%dT%H:%M:%S')

            try:
                start_time = datetime.strptime(self.log_ad_buffer[0][timestamp], '%Y-%m-%dT%H:%M:%S.%f')
            except Exception:
                start_time = datetime.strptime(self.log_ad_buffer[0][timestamp], '%Y-%m-%dT%H:%M:%S')

            if (end_time - start_time).seconds >= 60:
                result = self._process_buffer()
                self._reset_state()

        return result

    @synchronized
    def _process_buffer(self):
        log_count_buffer_copy = self.log_count_buffer.copy()
        log_ad_buffer_copy = self.log_ad_buffer.copy()
        self.log_count_buffer = []
        self.log_ad_buffer = []
        return self.model.get_incident_properties(log_count_buffer_copy, log_ad_buffer_copy)

    def _timeout_call(self):
        logger.debug("Initiating timer in incidents")
        result = self._process_buffer()
        if result is not None:
            self.data_sink.send(result)
        self._reset_state()

    def _reset_state(self):
        self.log_count_buffer = []
        self.log_ad_buffer = []
        self._reset_timer()

    def _reset_timer(self):
        self.timer.cancel()
        self.timer = threading.Timer(self.timeout_period, self._timeout_call)
        self.timer.name = self.module_name + '_timer'
        self.timer.start()
