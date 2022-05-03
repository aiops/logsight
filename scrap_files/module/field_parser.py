from __future__ import annotations

import logging
from typing import List, Optional, Union

from logsight_lib import FieldParser

from common.logsight_classes import LogsightLog
from pipeline.modules.core import AbstractHandler, Buffer, Context, Module, NamedTimer, State
from scrap_files.field_parsing.parser_provider import FieldParserProvider

logger = logging.getLogger("logsight." + __name__)


class FieldParsingModule(Module, Context, AbstractHandler):

    def flush(self, context: Optional[Union[List, LogsightLog]]) -> Optional[Union[List, LogsightLog]]:
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

    def transform(self, data: LogsightLog) -> LogsightLog:
        return self.process_context(data)

    def _handle(self, context: Optional[Union[List, LogsightLog]]) -> Optional[Union[List, LogsightLog]]:
        if context:
            return self.transform(context)


class CalibrationState(State):
    def flush(self, context: LogsightLog) -> Optional[List[LogsightLog]]:
        self.buffer.add(context)
        return self._process_buffer()

    def __init__(self, config):
        self.config = config
        self.buffer = Buffer(config.buffer_size)
        self.buffer_size = config.buffer_size
        self.timeout_period = config.timeout_period
        self.parser_provider = FieldParserProvider(config.provider_threshold)
        self.timer = NamedTimer(self.timeout_period, self.timeout_call, self.__class__.__name__)

    def handle(self, request: LogsightLog) -> Optional[List[LogsightLog]]:
        if request:
            self.buffer.add(request)
        if self.buffer.is_full:
            return self._process_buffer()
        return None

    def _process_buffer(self) -> Optional[List[LogsightLog]]:
        buffer_copy = self.buffer.flush_buffer()
        parser = self.parser_provider.get_parser(buffer_copy)
        logger.info(f"Field parser: {parser.type} {self.timer.name}")
        self.context.transition_to(FieldParserParseState(parser))
        self.timer.cancel()
        parser.parse_prev_timestamp(buffer_copy)
        result = [parser.parse_fields(log.event)[0] for log in buffer_copy]
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
    def flush(self, context: Optional[LogsightLog]) -> Optional[LogsightLog]:
        result = None
        if context:
            result = self.parser.parse_fields(context.event)[0]
        return result

    def __init__(self, parser: FieldParser):
        self.parser = parser

    def handle(self, request: Optional[Union[List, LogsightLog]]) -> Optional[Union[List, LogsightLog]]:
        if request:
            return self.parser.parse_fields(request.event)[0]
