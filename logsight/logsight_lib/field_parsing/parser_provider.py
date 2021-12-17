import logging
from copy import deepcopy
from typing import List, Dict

from logsight_lib.field_parsing.field_parser import GrokParser, JSONParser, NoParser
from logsight_lib.field_parsing.grok import Grok, read_grok_datetime_parsers

logger = logging.getLogger("logsight." + __name__)


class FieldParserProvider:
    def __init__(self, provider_threshold: float = 0.5):
        self._threshold = provider_threshold

        self.parsers = [
            JSONParser(),
            GrokParser("hadoop", Grok('%{HADOOP}', full_match=True, required_fields=['timestamp'])),
            GrokParser("syslog", Grok('%{SYSLOGLINE}', full_match=True, required_fields=['timestamp'])),
        ]
        self.parsers + [GrokParser(k, v) for k, v in read_grok_datetime_parsers().items()]

    def get_parser(self, logs: List[Dict]):
        if not logs:
            return NoParser()
        _logs = deepcopy(logs)
        for parser in self.parsers:
            # Do parsing and keep only not-None entries. Parsing was successful if result is not None
            results = [parser.parse_fields(log)[1] for log in _logs]
            results = list(filter(lambda x: x, results))
            ratio = len(results) / len(_logs)
            if ratio > self._threshold:
                logger.info(f"Identified field parser: {parser.type}")
                return parser
        return NoParser()
