import datetime
import logging

from parsing.grok import Grok, read_grok_datetime_parsers
from parsing.log import Log


class _FieldParser:
    def __init__(self, log_type: str, grok_parser: Grok, message_field: str = None):
        self.log_type = log_type
        self.message_parser = grok_parser
        self.message_field = message_field

    def parse_fields(self, log: Log):
        log.set_log_type(self.log_type)
        parsed_fields = self.message_parser.parse(log.get_message())
        if parsed_fields:
            log.update(parsed_fields)
            log.unify_log_representation()
            if self.message_field:
                log.map_message_field(self.message_field)
            success = True
        else:
            log.add_failed_grok_parsing(self.log_type)
            success = False
        return success, log


class FieldParser:
    def __init__(self, initial_search: int = 100):
        self.grok_field_parsers = {
            "syslog": Grok('%{SYSLOGLINE}', full_match=False, required_fields=['timestamp'])
        }
        self.grok_datetime_parsers = read_grok_datetime_parsers()

        self.initial_search = initial_search
        self.field_parser = None
        self.datetime_parser = None
        self._prev_timestamp = None

        self.initial_parser_providers = [
            lambda _: self.field_parser,
            self._provide_field_parser,
            lambda log: self.datetime_parser if not log.is_datetime_set() else None,
            self._provide_datetime_parser,
        ]
        self.parser_providers = [
            lambda _: self.field_parser,
            lambda log: self.datetime_parser if not log.is_datetime_set() else None,
        ]
        self._no_parser = _FieldParser("no-parser", Grok('%{NOPARSER}', full_match=False))  # This is the last resort

    def parse(self, log: dict):
        log_obj = Log(log)
        if self._prev_timestamp:
            log_obj.set_prev_timestamp(self._prev_timestamp)

        if self.initial_search > 0:
            providers = self.initial_parser_providers
            self.initial_search -= 1
        else:
            providers = self.parser_providers

        success = False
        for provider in providers:
            if not provider:
                continue
            parser = provider(log_obj)
            if not parser:
                continue

            success, log_obj = self._parse(log_obj, parser)
            if success:
                break

        if not success:
            _, log_obj = self._parse(log_obj, self._no_parser)

        self._prev_timestamp = log_obj.get_datetime_str()
        return log_obj

    @staticmethod
    def _parse(log: Log, field_parser: _FieldParser):
        try:
            success, log = field_parser.parse_fields(log)
        except ValueError as e:
            logging.info("Error occurred while parsing log message %s. Reason: %s", log, e)
            success = False
        return success, log

    def _provide_field_parser(self, log: Log):
        log_type = log.get_log_type()
        if log_type and log_type in self.grok_field_parsers:
            grok_parser = self.grok_field_parsers[log_type]
            parser = _FieldParser(log_type, grok_parser)
        else:
            parser = self._infer_parser(log.get_message(), self.grok_field_parsers)
        if parser:
            self.field_parser = parser
        return parser

    def _provide_datetime_parser(self, log: Log):
        if not log.is_datetime_set():
            parser = self._infer_parser(log.get_message(), self.grok_datetime_parsers)
            if parser:
                self.datetime_parser = parser
            return parser
        else:
            return None

    def _infer_parser(self, message: str, search_pool: dict):
        log_type, grok_parser = self._infer_grok_parser(message, search_pool)
        if not grok_parser:
            return None
        else:
            return _FieldParser(log_type, grok_parser)

    @staticmethod
    def _infer_grok_parser(message: str, search_pool: dict):
        logging.info("Trying to automatically infer the parser for log message %s", message)
        matched = [key for key, par in search_pool.items() if par.parse(message)]
        if len(matched) == 0:
            logging.info("Unable to infer a grok parser.")
            return None, None
        elif len(matched) > 1:
            logging.info("Multiple parsers [ %s ] are matching the log message %s.",
                         ",".join(matched), message)
        return matched[0], search_pool[matched[0]]


# if __name__ == '__main__':
#     import os
#     from glob import glob
#     import codecs
#
#     log_dir_path = "/home/alex/workspace_startup/customers/bdr/log_dir/test"
#     apps = next(os.walk(log_dir_path))[1]
#
#     logs = {}
#     for i, app in enumerate(apps):
#         fpp = FieldParser()
#         print("Processing app {}. It is {} / {}".format(app, i + 1, len(apps)))
#         if app not in logs:
#             logs[app] = []
#         app_log_dir = os.path.join(log_dir_path, app)
#         app_files = []
#         for d, _, _ in os.walk(app_log_dir):
#             app_files.extend([f for f in glob(os.path.join(d, '*')) if os.path.isfile(f)])
#
#         for ii, f in enumerate(app_files):
#             print("Processing file {}. It is {} / {}".format(f, ii + 1, len(app_files)))
#             with codecs.open(f, mode='r', errors='ignore') as fp:
#                 while True:
#                     try:
#                         line = fp.readline()
#                     except:
#                         continue
#                     if not line:
#                         break
#                     log_result = fpp.parse({"private_key": "key", "app_name": app, "message": line})
#                     logs[app].append(log_result)
#
#                     print(log_result.log)
