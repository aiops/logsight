from abc import ABC
from datetime import datetime
import threading


from modules.api import State
from modules.field_parser.parsing.field_parser import FieldParser
from modules.field_parser.parsing.log import Log
from modules.field_parser.parsing.parser_provider import FieldParserProvider


def synchronized(func):
    """Makes functions thread-safe"""
    func.__lock__ = threading.Lock()

    def synced_func(*args, **kws):
        with func.__lock__:
            return func(*args, **kws)

    return synced_func


class FieldParserParseState(State):
    def __init__(self, parser: FieldParser, configs):
        self.configs = configs
        self.parser = parser

        self._prev_time = datetime.now()

    @synchronized
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

    @synchronized
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

    @synchronized
    def _process_buffer(self):
        parser = self.parser_provider.get_parser(self.buffer)
        parser_state = FieldParserParseState(parser, self.configs)
        results = [parser_state.process(element.log) for element in self.buffer]

        return results, parser_state

