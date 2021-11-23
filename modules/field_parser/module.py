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
from .parsing.field_parser import FieldParser

logger = logging.getLogger("logsight." + __name__)


class FieldParserModule(StatefulModule):
    def __init__(self, data_source: Source, data_sink: Sink, internal_source: Source, internal_sink: Sink,
                 config):
        super().__init__(data_source, data_sink, internal_source, internal_sink)
        self.parser = FieldParser()
        self.module_name = "log_parsing"

    @synchronized
    def process_input(self, input_data):
        return self.parser.parse(input_data).log
