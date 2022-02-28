import datetime
import json
import logging
from functools import partial
from operator import is_not
from typing import Dict, List, Optional

from dateutil import parser

logger = logging.getLogger("logsight." + __name__)


class Log:
    def __init__(self, log: Dict):
        # A log message needs to have at least these fields
        self.required_fields = [
            'private_key',
            'app_name',
            'message'
        ]
        # Mappings used to unify the log level.
        self.level_mappings = {
            "warn": "WARN",
            "warning": "WARN",

            "info": "INFO",
            "fine": "INFO",
            "finer": "INFO",
            "debug": "INFO",

            "err": "ERROR",
            "error": "ERROR",
            "exception": "ERROR",

            "severe": "SEVERE"
        }
        # Possible keys that contain the timestamp. They are processed sequentially, first hit wins.
        self.timestamp_keys = [
            '@timestamp',
            'timestamp',
            'timestamp_iso8601',
            'EventTime',
            '_prev_timestamp'
        ]
        # Possible keys that contain the log severity. They are processed sequentially, first hit wins.
        self.level_keys = ["level", "severity", "Severity"]
        self.default_level = "INFO"

        self.log = log
        self._clean_message()
        # Verify the expected format of the log message. E.g. some keys are required, unification and field parsing
        self._verify_log()

    def _verify_log(self):
        # Check if required fields are missing
        missing_required_fields = [field for field in self.required_fields if field not in self.log]
        if missing_required_fields:
            raise ValueError(
                "Log verification failed. Missing required fields [ {} ] in log message {}.".format(
                    ",".join(missing_required_fields), self.log))

    def _clean_message(self):
        message = self.get_message().replace("\n", "").replace("\r", "")
        self.set_message(message)

    def set_timestamp(self, timestamp):
        if timestamp:
            self.log['@timestamp'] = self._format_timestamp(timestamp)

    def _format_timestamp(self, timestamp):
        if isinstance(timestamp, datetime.datetime):
            timestamp = timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')
        return timestamp

    def set_log_level(self, log_level: str):
        self.log['actual_level'] = log_level

    def set_field_parser_type(self, field_parser_type: str):
        self.log['field_parser'] = field_parser_type

    def tag_failed_field_parsing(self, identifier: str):
        if identifier:
            if '_failed_field_parsing' not in self.log:
                self.log['_failed_field_parsing'] = []
            self.log['_failed_field_parsing'].append(identifier)

    def set_message(self, message: str):
        self.log['message'] = message

    def set_prev_timestamp(self, timestamp):
        self.log["_prev_timestamp"] = self._format_timestamp(timestamp)

    def get_timestamp(self):
        return self.get_or_none('@timestamp')

    def get_log_level(self):
        return self.get_or_none('actual_level')

    def get_message(self):
        return self.get_or_none('message')

    def get_private_key(self):
        return self.get_or_none('private_key')

    def get_app(self):
        return self.get_or_none('app_name')

    def get_or_none(self, key):
        return self.log.get(key, None)

    def contains(self, key: str):
        return key in self.log

    def update(self, fields: dict):
        self.log.update(fields)
        return self

    def unify_log_representation(self):
        # Unify time and log level formats
        self._unify_time_format()
        self._unify_log_level()
        # Exclude all fields that are None
        self.log = dict(filter(lambda item: item[1] is not None, self.log.items()))

    def _unify_time_format(self):
        # Pop all datetime keys from log
        datetime_strings = [self.log.pop(key) for key in self.timestamp_keys if key in self.log]
        datetime_strings = [dts for dts in datetime_strings if dts]  # Filter None values
        if datetime_strings:
            timestamp = None
            for datetime_string in datetime_strings:
                try:
                    timestamp = parser.parse(datetime_string)
                except Exception as e:
                    logger.debug(f"Unable to parse datetime string {datetime_string}. Reason: {e}")
                    continue
                break
            if not timestamp:
                logger.debug(f"Unable to parse candidate datetime strings {datetime_strings}.")
                timestamp = None
        else:
            logger.debug(f"No timestamp key found for log {self.log}")
            timestamp = None

        self.set_timestamp(timestamp)

    def _unify_log_level(self):
        levels = self._get_possible_log_levels()
        if levels:
            level = levels[0]
        else:
            level = self._infer_log_level_from_message(self.get_message())

        self.set_log_level(level)

    def _get_possible_log_levels(self):
        levels = []
        for level in [self.log.get(key).lower() for key in self.level_keys if key in self.log]:
            levels.append(self.level_mappings.get(level, level))
        return levels

    def _infer_log_level_from_message(self, message):
        # logger.info("Trying to infer log level from log message...")
        level = next((self.level_mappings.get(key) for key in self.level_mappings if key in message),
                     self.default_level)
        # logger.info("Inferred log level: %s", level)
        return level

    def to_json_string(self):
        return json.dumps(self.log)


def dicts_to_logs(logs: List[Dict]) -> List[Log]:
    log_objs = [Log(log) for log in logs]
    return list(filter(partial(is_not, None), log_objs))


def dict_to_log(log: Dict) -> Optional[Log]:
    try:
        log_obj = Log(log)
    except Exception as e:
        logger.debug(f"Failed to instantiate log object from dict {log}: {e}")
        return None
    return log_obj
