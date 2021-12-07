from __future__ import annotations

import logging
from typing import Any, Optional
from modules.core import AbstractHandler, Context, State, Module
from modules.core.buffer import Buffer
from modules.core.timer import NamedTimer
from logsight_lib.anomaly_detection.log_anomaly_detection import LogAnomalyDetector

logger = logging.getLogger("logsight." + __name__)


class ModelNotLoadedException(Exception):
    pass


class AnomalyDetectionModule(Module, Context, AbstractHandler):
    """
    Performs Anomaly detection on log data
    """
    module_name = "ad"

    def __init__(self, config):
        self.timeout_period = config.timeout_period
        Context.__init__(self, IdleState(config))

    def start(self):
        self._state.handle(None)  # Moves from Init State to Loaded state automatically

    def _process_data(self, data: Any) -> Optional[Any]:
        if isinstance(self._state, IdleState):
            raise ModelNotLoadedException("Please load a model first.")
        if data:
            return self.process_context(data)

    def handle(self, request: Any) -> Optional[str]:
        if request:
            try:
                result = self._process_data(request)
                if self._next_handler:
                    if result:
                        logger.debug(f"Sending {len(result)}")
                    return self._next_handler.handle(result)
                return result
            except ModelNotLoadedException as e:
                logger.error(e)


class IdleState(State):
    def __init__(self, config):
        self.config = config
        self.ad = LogAnomalyDetector()

    def handle(self, request: Optional[Any] = None) -> Optional[Any]:
        try:
            self.ad.load_model(None, None)
        except Exception as e:
            logging.log(logging.ERROR, e)
        return self.context.transition_to(LoadedState(self.ad, self.config))


class LoadedState(State):
    def __init__(self, ad: LogAnomalyDetector, config):
        self.ad = ad
        self.config = config
        self.buffer = Buffer(config.buffer_size)
        self.timer = NamedTimer(self.config.timeout_period, self.timeout_call, self.__class__.__name__).start()

    def handle(self, request: Any) -> Optional[Any]:
        if isinstance(request, list):
            self.buffer.extend(request)
        else:
            self.buffer.add(request)
        if self.buffer.is_full:
            return self.ad.process_log(self.buffer.flush_buffer())

    @property
    def context(self) -> AnomalyDetectionModule:
        return self._context

    @context.setter
    def context(self, context: AnomalyDetectionModule) -> None:
        self._context = context

    def timeout_call(self):
        logger.debug("Initiating timer")
        if not self.buffer.is_empty:
            result = self.ad.process_log(self.buffer.flush_buffer())
            if self.context.next_handler:
                if result:
                    logger.debug(f"Sending {len(result)}")

                self.context.next_handler.handle(result)
        self.timer.reset_timer()
