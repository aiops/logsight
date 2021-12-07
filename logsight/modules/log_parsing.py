from __future__ import annotations

import logging
from typing import Any, Optional, List
from modules.core import AbstractHandler, Context, State, Module
from modules.core.buffer import Buffer
from logsight_lib.log_parsing import Parser, DrainLogParser
from modules.core.timer import NamedTimer

logger = logging.getLogger("logsight." + __name__)


class LogParserModule(Module, Context, AbstractHandler):
    """
    Performs Anomaly detection on log data
    """
    module_name = "log_parsing"

    def __init__(self, config):
        Context.__init__(self, TrainState(DrainLogParser(), config))

    def start(self):
        super().start()
        if isinstance(self._state, TrainState):
            self._state.timer.start()

    def _process_data(self, data: Any) -> Optional[Any]:
        if data:
            return self.process_context(data)

    def handle(self, request: Any) -> Optional[str]:
        result = self._process_data(request)
        if self.next_handler:
            return self._next_handler.handle(result)
        return result


class TrainState(State):

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
        self.context.transition_to(PredictState(self.parser, self.config))
        self.timer.cancel()
        return result

    def timeout_call(self):
        result = self._process_buffer()
        if self.context.next_handler:
            self.context.next_handler.handle(result)


class PredictState(State):

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