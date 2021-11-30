import json
import logging
import threading

from connectors.sinks import Sink, PrintSink, KafkaSink
from connectors.sources import Source, PrintSource, FileSource
from modules.core import StatefulModule
from modules.core.wrappers import synchronized
from modules.field_parser.states import FieldParserCalibrationState

logger = logging.getLogger("logsight." + __name__)


class FieldParserModule(StatefulModule):
    def process_internal_message(self, msg):
        # Not yet implemented
        pass

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

    def _timeout_call(self):
        logger.debug("Field parsing calibration timeout occurred. Switching to parsing state.")
        result, self.state = self.state.next_state()
        self.data_sink.send(result)

    def process_input(self, input_data):
        result, self.state = self.state.process(input_data)
        return result


if __name__ == '__main__':
    import logging.config
    logging.config.dictConfig(json.load(open("../../config/log.json", 'r')))

    class Configs:
        state_configs = {
            "buffer_size": 10,
            "provider_threshold": 0.5
        }
        timeout_period = 5


    sink = PrintSink()
    stngs = {
        "host": "localhost",
        "port": "9093",
        "offset": "latest",
        "topic": "internal",
        "address": "localhost:9093"
    }
    sink_data = KafkaSink(**stngs)
    src_data = FileSource('/home/alex/Downloads/aaa/ten.json')

    module = FieldParserModule(src_data, sink_data, None, sink, Configs())
    module.run()
