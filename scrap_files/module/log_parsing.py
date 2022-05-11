from __future__ import annotations

import logging
from typing import Any, List, Optional, Union

from logsight_lib import DrainLogParser, Parser

from common.logsight_classes import LogsightLog
from pipeline.modules.core import AbstractHandler, Context, Module, State
from pipeline.modules.core import Buffer
from pipeline.modules.core import NamedTimer

logger = logging.getLogger("logsight." + __name__)


class LogParserModule(Module, Context, AbstractHandler):
    """
    Performs Anomaly detection on log data
    """

    module_name = "log_parsing"

    def __init__(self, config, app_settings=None):
        Context.__init__(self, TrainState(DrainLogParser(), config))
        Module.__init__(self)
        AbstractHandler.__init__(self)

    def start(self, ctx: dict):
        ctx["module"] = self.module_name
        super().start(ctx)
        if isinstance(self._state, TrainState):
            self._state.timer.start()

    def transform(self, data: Optional[Union[List, LogsightLog]]) -> Optional[Union[List, LogsightLog]]:
        if data:
            return self.process_context(data)

    def _handle(self, context: Optional[Union[List, LogsightLog]]) -> Optional[Union[List, LogsightLog]]:
        return self.transform(context)

    def flush(self, context: Optional[Union[List, LogsightLog]]) -> Optional[Union[List, LogsightLog]]:
        result = self.flush_state(context)
        return super().flush(result)


class TrainState(State):

    def flush(self, context: Optional[Union[List, LogsightLog]]) -> Optional[Any]:
        if context:
            if isinstance(context, list):
                self.buffer.extend(context)
            else:
                self.buffer.add(context)
        result = self._do_parsing()
        return result

    def __init__(self, parser: Parser, config):
        self.parser = parser
        self.config = config
        self.buffer = Buffer(config.buffer_size)
        self.timer = NamedTimer(config.timeout_period, self.timeout_call, self.__class__.__name__)

    def handle(self, request: Any) -> Optional[Any]:
        if isinstance(request, list):
            self.buffer.extend(request)
        else:
            self.buffer.add(request)
        if self.buffer.is_full:
            return self._process_buffer()

    def _process_buffer(self) -> Optional[List[dict]]:
        result = self._do_parsing()
        self.context.transition_to(PredictState(self.parser, self.config))
        self.timer.cancel()
        return result

    def _do_parsing(self):
        result = None
        if not self.buffer.is_empty:
            flushed_buffer = self.buffer.flush_buffer()
            _ = [self.parser.parse(item) for item in flushed_buffer]
            # parse again after training
            result = []
            for item in flushed_buffer:
                res = self.parser.parse(item)
                if res:
                    result.append(res)
        return result

    def timeout_call(self):
        result = self._process_buffer()
        if self.context.next_handler:
            self.context.next_handler.handle(result)


class PredictState(State):

    def flush(self, context: Optional[Any]) -> Optional[Any]:
        if context:
            return self.handle(context)
        return context

    def __init__(self, parser: Parser, config):
        self.parser = parser
        self.config = config
        self.predict_limit = config.retrain_after
        self.parse_count = 0
        self.parser.set_state(Parser.TEST_STATE)

    def handle(self, request: Any) -> Optional[Any]:
        if not isinstance(request, list):
            request = [request]
        result = []
        for req in request:
            self.parse_count += 1
            res = self.parser.parse(req)
            if res:
                result.append(res)
        if self.parse_count >= self.predict_limit:
            self.context.transition_to(TuneState(self.parser, self.config))
        return result


class TuneState(State):

    def flush(self, context: Optional[Any]) -> Optional[Any]:
        result = self.handle(context)
        logger.debug(f"Flushed {len(result)} messages.")
        return result

    def __init__(self, parser: Parser, config):
        self.parser = parser
        self.config = config
        self.retrain_size = config.buffer_size
        self.parse_count = 0
        self.parser.set_state(Parser.TUNE_STATE)

    def handle(self, request: Any) -> Optional[Any]:
        if not isinstance(request, list):
            request = [request]
        result = []
        for req in request:
            self.parse_count += 1
            res = self.parser.parse(req)
            if res:
                result.append(res)
        if self.parse_count >= self.retrain_size:
            self.context.transition_to(PredictState(self.parser, self.config))

        return result
