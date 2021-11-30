from abc import ABC
from datetime import datetime
import threading

from modules.core import State
from modules.core.wrappers import synchronized
from logsight_lib.field_parsing.field_parser import FieldParser
from logsight_lib.field_parsing.log import Log
from logsight_lib.field_parsing.parser_provider import FieldParserProvider


class FieldParserParseState(State):
    def __init__(self, parser: FieldParser, configs):
        self.configs = configs
        self.parser = parser

        self._prev_time = datetime.now()

    def process(self, data: dict):
        log = Log(data)

        log.set_field_parser_type(self.parser.type)
        log.set_prev_timestamp(self._prev_time)

        parsed_message = self.parser.parse_fields(log.get_message())
        if parsed_message:
            log.update(parsed_message)
        else:
            log.tag_failed_field_parsing(self.parser.type)

        log.unify_log_representation()
        self._prev_time = log.get_timestamp()

        return log.log, self

    def next_state(self):
        return None, self


class FieldParserCalibrationState(State):
    def __init__(self, configs):
        self.configs = configs

        self.buffer = []
        self.buffer_size = configs['buffer_size']

        self.parser_provider = FieldParserProvider(configs)

    def process(self, data):
        state = self
        results = []

        log = Log(data)
        self.buffer.append(log)

        if len(self.buffer) == self.buffer_size:
            results, state = self._process_buffer()

        return results, state

    def next_state(self):
        return self._process_buffer()

    def _process_buffer(self):
        parser = self.parser_provider.get_parser(self.buffer)
        parser_state = FieldParserParseState(parser, self.configs)
        results = [parser_state.process(element.log)[0] for element in self.buffer]

        return results, parser_state
