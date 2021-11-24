import logging
import threading

from connectors.sink import Sink, PrintSink
from connectors.source import Source, PrintSource, FileSource
from modules.api import StatefulModule
from modules.api.wrappers import synchronized
from modules.field_parser.states import FieldParserCalibrationState

logger = logging.getLogger("logsight." + __name__)


class FieldParserModule(StatefulModule):
    def __init__(self, data_source: Source, data_sink: Sink, internal_source: Source, internal_sink: Sink,
                 config):
        super().__init__(data_source, data_sink, internal_source, internal_sink)
        self.timeout_period = config.timeout_period
        self.state = FieldParserCalibrationState(config.state_configs)
        self.module_name = "field_parsing"
        self.timer = None

    def run(self):
        super().run()
        self.timer = threading.Timer(self.timeout_period, self._timeout_call)
        self.timer.start()

    @synchronized
    def _timeout_call(self):
        logger.debug("Field parsing calibration timeout occurred. Switching to parsing state.")
        result, self.state = self.state.next_state()
        self.data_sink.send(result)

    @synchronized
    def process_input(self, input_data):
        result, self.state = self.state.process(input_data)
        return result


if __name__ == '__main__':
    class Configs:
        state_configs = {
            "buffer_size": 10,
            "provider_threshold": 0.5
        }
        timeout_period = 5


    sink = PrintSink()
    sink_data = PrintSink()

    #src_data = FileSource('/var/log/syslog')
    src_data = FileSource('/home/alex/Downloads/aaa/ten.json')

    module = FieldParserModule(src_data, sink_data, None, sink, Configs())
    module.run()
