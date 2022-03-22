from __future__ import annotations

import logging
import traceback
from copy import deepcopy
from typing import Any, Optional
from modules.core import AbstractHandler, Context, State, Module
from modules.core.buffer import Buffer
from modules.core.timer import NamedTimer
from logsight_lib.anomaly_detection import log_anomaly_detection
from logsight_lib.anomaly_detection.log_anomaly_detection import LogAnomalyDetector

logger = logging.getLogger("logsight." + __name__)


class ModelNotLoadedException(Exception):
    pass


class AnomalyDetectionModule(Module, Context, AbstractHandler):
    """
    Performs Anomaly detection on log data
    """

    def flush(self, context=None) -> Optional[str]:
        result = self.flush_state(context)
        return super().flush(result)

    module_name = "anomaly_detection"

    def __init__(self, config, app_settings=None):
        Context.__init__(self, IdleState(config))
        Module.__init__(self)
        AbstractHandler.__init__(self)

        self.app_settings = app_settings
        self.timeout_period = config.timeout_period

    def start(self, ctx: dict):
        ctx["module"] = self.module_name
        super().start(ctx)
        self._state.handle(None)  # Moves from Init State to Loaded state automatically

    def _process_data(self, data: Any) -> Optional[Any]:
        if isinstance(self._state, IdleState):
            raise ModelNotLoadedException("Please load a model first.")
        if data:
            return self.process_context(data)

    def _handel(self, request: Any) -> Optional[str]:
        result = None
        try:
            result = self._process_data(request)
        except ModelNotLoadedException as e:
            logger.error(e)
        return result


class IdleState(State):
    def flush(self, **kwargs) -> Optional[Any]:
        return

    def __init__(self, config):
        self.config = config
        self.detector = config.detector
        self.ad = getattr(log_anomaly_detection, self.detector)()

    def handle(self, request: Optional[Any] = None) -> Optional[Any]:
        try:
            self.ad.load_model(None, None)
        except Exception as e:
            logger.error(e)
        return self.context.transition_to(LoadedState(self.ad, self.config))


class LoadedState(State):

    def __init__(self, ad: LogAnomalyDetector, config):
        self.ad = ad
        self.config = config
        self.buffer = Buffer(config.buffer_size)
        self.timer = NamedTimer(self.config.timeout_period, self.timeout_call, self.__class__.__name__).start()

    def flush(self, context) -> Optional[Any]:
        if context:
            if isinstance(context, list):
                self.buffer.extend(context)
            else:
                self.buffer.add(context)
        if not self.buffer.is_empty:
            return self.ad.process_log(self.buffer.flush_buffer())

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
        logger.debug(f"Initiating timer for app {self.context.app_settings.application_name}")
        if not self.buffer.is_empty:
            result = self.ad.process_log(self.buffer.flush_buffer())
            if self.context.next_handler:
                self.context.next_handler.handle(result)
        self.timer.reset_timer()
