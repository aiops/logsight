from typing import Callable

import ujson as ujson

from analytics_core.logs import LogsightLog
from pipeline.modules.core.module import TransformModule


class FieldParser(TransformModule):
    """ TODO: @Alex explain what this class does."""

    def __init__(self, **kwargs):
        self.__dict__.update(**kwargs)
        super().__init__()

    def _get_transform_function(self) -> Callable[[LogsightLog], LogsightLog]:
        return self._parse_jsons

    @staticmethod
    def _parse_jsons(log: LogsightLog) -> LogsightLog:
        parsed = ujson.loads(log.event.message)
        log.event.timestamp = parsed['@timestamp']
        log.event.level = parsed['Severity']
        log.event.message = parsed['message']
        return log
