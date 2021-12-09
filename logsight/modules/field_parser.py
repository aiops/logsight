from __future__ import annotations

import logging
from typing import Any, Optional, List, Dict

from logsight_lib.field_parsing import FieldParser
from logsight_lib.field_parsing.parser_provider import FieldParserProvider
from modules.core import AbstractHandler, Context, State, Module
from modules.core.buffer import Buffer
from modules.core.timer import NamedTimer

logger = logging.getLogger("logsight." + __name__)


class FieldParsingModule(Module, Context, AbstractHandler):
    module_name = "field_parsing"

    def __init__(self, config, app_settings=None):
        self.app_settings = app_settings
        Context.__init__(self, CalibrationState(config))

    def start(self):
        super().start()
        if isinstance(self._state, CalibrationState):
            self._state.timer.start()

    def _process_data(self, data: Dict) -> Optional[Dict]:
        if data:
            return self.process_context(data)

    def handle(self, request: Any) -> Optional[str]:
        result = self._process_data(request)
        if self.next_handler:
            return self._next_handler.handle(result)
        return result


class CalibrationState(State):
    def __init__(self, config):
        self.config = config
        self.buffer = Buffer(config.buffer_size)
        self.buffer_size = config.buffer_size
        self.timeout_period = config.timeout_period
        self.parser_provider = FieldParserProvider(config.provider_threshold)
        self.timer = NamedTimer(self.timeout_period, self.timeout_call, self.__class__.__name__)

    def handle(self, request: Optional[Dict]) -> Optional[List[str]]:
        if request:
            self.buffer.add(request)
        if self.buffer.is_full:
            return self._process_buffer()
        return None

    def _process_buffer(self):
        buffer_copy = self.buffer.flush_buffer()
        if not buffer_copy:
            return
        parser = self.parser_provider.get_parser(buffer_copy)
        logger.info(f"Field parser: {parser.type} {self.timer.name}")
        self.context.transition_to(FieldParserParseState(parser))
        self.timer.cancel()
        parser.parse_prev_timestamp(buffer_copy)
        return [parser.parse_fields(log)[0] for log in buffer_copy]

    def timeout_call(self):
        result = self._process_buffer()
        if self.context.next_handler:
            self.context.next_handler.handle(result)


class FieldParserParseState(State):
    def __init__(self, parser: FieldParser):
        self.parser = parser

    def handle(self, request: Dict) -> Optional[Dict]:
        if request:
            return self.parser.parse_fields(request)[0]
