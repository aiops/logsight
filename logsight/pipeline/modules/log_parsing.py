from __future__ import annotations

import logging
from typing import Callable

from analytics_core.logs import LogsightLog
from analytics_core.modules.log_parsing import DrainLogParser, Parser
from pipeline.modules.core import TransformModule

logger = logging.getLogger("logsight." + __name__)


class LogParserModule(TransformModule):
    def __init__(self):
        self.parser = DrainLogParser()
        super().__init__()

    def _get_transform_function(self) -> Callable[[LogsightLog], LogsightLog]:
        return self.parser.parse

    def set_parser(self, parser: Parser):
        self.parser = parser
