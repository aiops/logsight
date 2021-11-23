import json
import logging
import threading
from time import time

from connectors.sink import Sink
from connectors.source import Source
from modules.api import StatefulModule, State
from modules.log_parsing.parsers import DrainLogParser, TestParser
from modules.log_parsing.states import ParserTrainState, ParserPredictState, ParserTuneState, Status
from modules.api.wrappers import synchronized

logger = logging.getLogger("logsight." + __name__)


class ParserModule(StatefulModule):
    def __init__(self, data_source: Source, data_sink: Sink, internal_source: Source, internal_sink: Sink,
                 config):
        super().__init__(data_source, data_sink, internal_source, internal_sink)

        self.timeout_period = config.timeout_period
        self.state = ParserTrainState(DrainLogParser(), config.state_configs)
        self.module_name = "log_parsing"
        self.timer = None

    def run(self):
        super().run()
        self.timer = threading.Timer(self.timeout_period, self._timeout_call)
        self.timer.start()

    @synchronized
    def process_input(self, input_data):
        # Process data based on internal state
        result, status = self.state.process(input_data)
        if status == Status.MOVE_STATE:
            self.state = self.state.next_state()
            self._reset_timer()
        return result

    @synchronized
    def _timeout_call(self):
        logger.debug("Initiating timer LogParse.")
        result, status = self.state.finish_state()
        self.data_sink.send(result)
        self.state = self.state.next_state()

        return result

    def _reset_timer(self):
        self.timer.cancel()
        self.timer = threading.Timer(self.timeout_period, self._timeout_call)
        self.timer.start()
