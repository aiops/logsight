import json
import threading
from connectors.sink import Sink
from connectors.source import Source
from modules.api import StatefulModule, State
from modules.log_parsing.parsers import DrainLogParser, TestParser
from modules.log_parsing.states import ParserTrainState, ParserPredictState, ParserTuneState, Status
from modules.api.wrappers import synchronized


class ParserModule(StatefulModule):
    def __init__(self, data_source: Source, data_sink: Sink, internal_source: Source, internal_sink: Sink,
                 config):
        super().__init__(data_source, data_sink, internal_source, internal_sink)

        self.timeout_period = config.timeout_period
        self.state = ParserTrainState(DrainLogParser(), config.state_configs)
        self.module_name = "log_parsing"
        self.timer = None

    def run(self):
        self.internal_source.connect()
        internal = threading.Thread(target=self.start_internal_listener)
        internal.start()
        stream = threading.Thread(target=self.start_data_stream)
        stream.start()
        self.timer = threading.Timer(self.timeout_period, self._timeout_call)
        self.timer.start()

    @synchronized
    def process_input(self, input_data):
        # Process data based on internal state
        result, status = self.state.process(input_data)
        # if ready to move to the next state, change state
        if status == Status.MOVE_STATE:
            self.state = self.state.next_state()
            self.timer.cancel()
        print("Current state:", self.state.__class__.__name__)
        return result

    @synchronized
    def _timeout_call(self):
        print("Initiating timer")
        result, status = self.state.finish_state()
        self.data_sink.send(result)
        self.state = self.state.next_state()

        return result
