from __future__ import annotations

import logging

from analytics_core.logs import LogBatch
from analytics_core.modules.log_parsing.mask_parser import MaskLogParser
from analytics_core.modules.log_parsing.parser import Parser
from pipeline.modules.core import TransformModule

logger = logging.getLogger("logsight." + __name__)


class LogParserModule(TransformModule):

    def __init__(self):
        super().__init__()
        self.parser = MaskLogParser()

    def transform(self, data: LogBatch) -> LogBatch:
        data.logs = self.parser.parse(data.logs)
        return data

    def set_parser(self, parser: Parser):
        self.parser = parser
