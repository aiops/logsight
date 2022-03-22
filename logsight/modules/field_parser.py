from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any, Optional, List, Dict

from logsight_lib.field_parsing import FieldParser
from logsight_lib.field_parsing.parser_provider import FieldParserProvider
from modules.core import AbstractHandler, Context, State, Module
from modules.core.buffer import Buffer
from modules.core.timer import NamedTimer

logger = logging.getLogger("logsight." + __name__)


class FieldParsingModule(Module, Context, AbstractHandler):

    def flush(self, context: Optional[Any]) -> Optional[str]:
        return super().flush(self.flush_state(context))

    module_name = "field_parsing"

    def __init__(self, config, app_settings=None):
        Context.__init__(self, CalibrationState(config))
        Module.__init__(self)
        AbstractHandler.__init__(self)

        self.app_settings = app_settings

    def start(self, ctx: dict):
        ctx["module"] = self.module_name
        super().start(ctx)
        if isinstance(self._state, CalibrationState):
            self._state.timer.start()

    def _process_data(self, data: Dict) -> Optional[Dict]:
        if data:
            return self.process_context(data)

    def _handel(self, request) -> Optional[str]:
        return self._process_data(request)


class CalibrationState(State):
    def flush(self, context: Optional[Any]) -> Optional[Any]:
        if context:
            self.buffer.add(context)
        return self._process_buffer()

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
        result = [parser.parse_fields(log)[0] for log in buffer_copy]
        result = [r for r in result if r]
        return result if result else None

    def timeout_call(self):
        if not self.buffer.is_empty:
            result = self._process_buffer()
            if self.context.next_handler:
                self.context.next_handler.handle(result)
        else:
            self.timer.reset_timer()


class FieldParserParseState(State):
    def flush(self, context: Optional[Any]) -> Optional[Any]:
        result = None
        if context:
            result = self.parser.parse_fields(context)[0]
        return result

    def __init__(self, parser: FieldParser):
        self.parser = parser

    def handle(self, request: Dict) -> Optional[Dict]:
        if request:
            return self.parser.parse_fields(request)[0]
