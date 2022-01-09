import copy
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
            JSONParser(log_parsing_fail=False),
            GrokParser(
                "hadoop",
                Grok('%{HADOOP}', full_match=True, required_fields=['timestamp']),
                log_parsing_fail=False),
            GrokParser(
                "syslog",
                Grok('%{SYSLOGLINE}', full_match=True, required_fields=['timestamp']),
                log_parsing_fail=False),
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


if __name__ == '__main__':
    l1 = {
        'private_key': 'xsle3p2syaoim8ilz15pfstye',
        'app_name': 'hdfs_node',
        'log_type': 'unknown_format',
        'message': '2021-12-16 05:25:32,359 INFO org.apache.hadoop.http.HttpServer2: Process Thread Dump: jsp requested'
    }
    l2 = {
        'private_key': 'xsle3p2syaoim8ilz15pfstye',
        'app_name': 'hdfs_node',
        'log_type': 'unknown_format',
        'message': '47 active threads'
    }
    l3 = {
        'private_key': 'xsle3p2syaoim8ilz15pfstye',
        'app_name': 'hdfs_node',
        'log_type': 'unknown_format',
        'message': 'Thread 65 (qtp262445056-65): State: TIMED_WAITING Blocked count: 0 Waited count: 19 Stack:'
    }

    logs = [copy.deepcopy(l1) for _ in range(100)]
    fpp = FieldParserProvider()
    g = fpp.get_parser(logs)

    r1 = g.parse_fields(l1)
    r2 = g.parse_fields(l2)
    r3 = g.parse_fields(l3)

    print(r1)
    print(r2)
    print(r3)
