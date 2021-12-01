import json
import logging
import threading
from time import time

from connectors.sinks import Sink
from connectors.sources import Source
from modules.core import StatefulModule

from modules.core.wrappers import synchronized
from logsight_lib.log_aggregation import LogAggregator

logger = logging.getLogger("logsight." + __name__)


class LogAggregationModule(StatefulModule):
    def process_internal_message(self, msg):
        return

    def __init__(self, data_source: Source, data_sink: Sink, internal_source: Source, internal_sink: Sink,
                 config):
        super().__init__(data_source, data_sink, internal_source, internal_sink)
        self.module_name = "log_aggregation"
        self.config = config
        self.buffer = []
        self.timeout_period = self.config.timeout_period
        self.timer = threading.Timer(self.timeout_period, self._timeout_call)
        self.timer.name = self.module_name + '_timer'
        self.aggregator = LogAggregator()

    def run(self):
        super().run()
        self.timer.start()

    @synchronized
    def process_input(self, input_data):
        # Process data based on internal state
        self.buffer.append(input_data)

    def _process_buffer(self):
        copy_buffer = self.buffer.copy()
        self.buffer = []
        result = self.aggregator.aggregate_logs(copy_buffer)
        self._reset_timer()
        return result

    def _timeout_call(self):
        logger.debug("Initiating timer LogAgg.")
        result = self._process_buffer()
        self.buffer = []
        if result:
            try:
                self.data_sink.send(result)
            except Exception as e:
                logger.error(f'{e}')

    def _reset_timer(self):
        self.timer.cancel()
        self.timer = threading.Timer(self.timeout_period, self._timeout_call)
        self.timer.name = self.module_name + '_timer'
        self.timer.start()
