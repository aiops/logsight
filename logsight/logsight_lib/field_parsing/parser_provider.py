from .field_parser import GrokParser, JSONParser, NoParser
from .grok import Grok, read_grok_datetime_parsers


class FieldParserProvider:
    def __init__(self, configs):
        self._threshold = configs.get('provider_threshold', 0.5)

        self.parsers = [
            JSONParser(),
            GrokParser("syslog", Grok('%{SYSLOGLINE}', full_match=True, required_fields=['timestamp'])),
        ]
        self.parsers + [GrokParser(k, v) for k, v in read_grok_datetime_parsers().items()]

    def get_parser(self, logs: list):
        if not logs:
            return NoParser()
        for parser in self.parsers:
            # Do parsing and keep only not-None entries. Parsing was successful if result is not None
            results = [parser.parse_fields(log.get_message()) for log in logs]
            results = list(filter(lambda x: x, results))
            ratio = len(results) / len(logs)
            if ratio > self._threshold:
                return parser
        return NoParser()
