import logging
from typing import Any, Optional

from modules.core import AbstractHandler, Module
from logsight_lib.log_aggregation import LogAggregator
from modules.core.buffer import Buffer
from modules.core.timer import NamedTimer

logger = logging.getLogger("logsight." + __name__)


class LogAggregationModule(Module, AbstractHandler):
    module_name = "log_aggregation"

    def __init__(self, config,app_settings=None):
        self.app_settings = app_settings
        self.config = config
        self.buffer = Buffer(config.buffer_size)
        self.timeout_period = self.config.timeout_period
        self.timer = NamedTimer(self.timeout_period, self.timeout_call, self.__class__.__name__)
        self.timer.name = self.module_name + '_timer'
        self.aggregator = LogAggregator()

    def start(self):
        super().start()
        self.timer.start()

    def _process_data(self, data: Any) -> Optional[Any]:
        if data:
            if isinstance(data, list):
                self.buffer.extend(data)
            else:
                self.buffer.add(data)

            if self.buffer.is_full:
                return self._process_buffer()

    def handle(self, request: Any) -> Optional[str]:
        if request:
            result = self._process_data(request)
            if self.next_handler:
                return self._next_handler.handle(result)
            return result

    def _process_buffer(self):
        result = self.aggregator.aggregate_logs(self.buffer.flush_buffer())
        self.timer.reset_timer()

        return result

    def timeout_call(self):
        logger.debug(f"Initiating timer for app {self.app_settings.application_name}")
        result = self._process_buffer()
        if self.next_handler:
            self.next_handler.handle(result)
