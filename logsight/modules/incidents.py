import logging
from copy import deepcopy
from datetime import datetime
from typing import Any, Optional

from modules.core import AbstractHandler, Module

from logsight_lib.incidents import IncidentDetector
from modules.core.buffer import Buffer
from modules.core.timer import NamedTimer

logger = logging.getLogger("logsight." + __name__)


class LogIncidentModule(Module, AbstractHandler):
    module_name = "incidents"

    def __init__(self, config, app_settings=None):
        Module.__init__(self)
        AbstractHandler.__init__(self)

        self.config = config
        self.timeout_period = config.timeout_period

        self.log_count_buffer = Buffer(config.buffer_size)
        self.log_ad_buffer = Buffer(config.buffer_size)
        self.timer = NamedTimer(self.timeout_period, self._timeout_call, self.__class__.__name__)

        self.model = IncidentDetector()

    def start(self, ctx: dict):
        ctx["module"] = self.module_name
        super().start(ctx)
        self.timer.start()

    def _process_data(self, data: Optional[dict]) -> Optional[Any]:
        if not data:
            return
        if 'timestamp_start' in data:
            if isinstance(data, list):
                self.log_count_buffer.extend(data)
            else:
                self.log_count_buffer.add(data)
            if float(data['prediction']) > 0 or len(data['new_templates']) > 0:
                self.timer.reset_timer()
                return self.model.get_incident_properties(self.log_count_buffer.flush_buffer(),
                                                          self.log_ad_buffer.flush_buffer())
        else:
            if isinstance(data, list):
                self.log_ad_buffer.extend(data)
            else:
                self.log_ad_buffer.add(data)
            timestamp = "@timestamp"
            end_time = self._parse_time(self.log_ad_buffer[-1][timestamp])
            start_time = self._parse_time(self.log_ad_buffer[0][timestamp])
            if (end_time - start_time).seconds >= 60:
                self.timer.reset_timer()
                return self.model.get_incident_properties(self.log_count_buffer.flush_buffer(),
                                                          self.log_ad_buffer.flush_buffer())

    def flush(self, context: Optional[Any]) -> Optional[str]:
        result = None
        if context:
            if 'timestamp_start' in context:
                if isinstance(context, list):
                    self.log_count_buffer.extend(context)
                else:
                    self.log_count_buffer.add(context)
            else:
                if isinstance(context, list):
                    self.log_ad_buffer.extend(context)
                else:
                    self.log_ad_buffer.add(context)
            result = self.model.get_incident_properties(self.log_count_buffer.flush_buffer(),
                                                        self.log_ad_buffer.flush_buffer())
        return super().handle(result)

    def handle(self, request: Any) -> Optional[str]:
        result = self._process_data(request)
        return super().handle(result)

    def _timeout_call(self):
        logger.debug("Initiating timer.")
        result = self.model.get_incident_properties(self.log_count_buffer.flush_buffer(),
                                                    self.log_ad_buffer.flush_buffer())
        self.timer.reset_timer()
        if self.next_handler:
            self.next_handler.handle(result)

    @staticmethod
    def _parse_time(timestamp):
        try:
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
        except ValueError:
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')
