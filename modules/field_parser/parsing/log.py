import json
import logging
import datetime
from json import JSONDecodeError
from copy import deepcopy

from dateutil import parser


def _str_to_json(string: str):
    try:
        log_message_json = json.loads(string, strict=False)
    except JSONDecodeError as e:
        return None
    return log_message_json


def _unwrap_json(dict_to_update, json_str):
    # Check if the message element in the log is a json. If yes, unwrap and merge it with outer json.
    json_dict = _str_to_json(json_str)
    if json_dict and isinstance(json_dict, dict):
        dict_to_update.update(json_dict)
    return dict_to_update


class Log:
    def __init__(self, log: dict):
        # A log message needs to have at least these fields
        self.required_fields = [
            'private_key',
            'app_name',
            'message'
        ]
        # This are special protected fields that cannot be altered by updating
        self.protected_fields = [
            'private_key',
            'app_name',
            '@timestamp'
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
            '_prev_timestamp'
        ]
        # Possible keys that contain the log severity. They are processed sequentially, first hit wins.
        self.level_keys = ["level", "severity", "Severity"]
        self.default_level = "INFO"

        self.log = deepcopy(log)
        # Verify the expected format of the log message. E.g. some keys are required, unification and field parsing
        self._verify_log()
        self.log = _unwrap_json(log, self.get_message())
        self._clean_message()
        self._verify_log()  # _unwrap_json manipulates the log object. thus, the double verification

    def _verify_log(self):
        # Check if required fields are missing
        missing_required_fields = [field for field in self.required_fields if field not in self.log]
        if missing_required_fields:
            raise ValueError(
                "Log verification failed. Missing required fields [ {} ] in log message {}.".format(
                    ",".join(missing_required_fields), self.log))
        return True

    def _clean_message(self):
        message = self.get_message()
        message = message.replace("\n", "").replace("\r", "")
        self.set_message(message)

    def is_datetime_set(self):
        return '@timestamp' in self.log

    def set_datetime(self, _datetime):
        if isinstance(_datetime, str):
            self.log['@timestamp'] = _datetime
        elif isinstance(_datetime, datetime.datetime):
            self.log['@timestamp'] = _datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')
        else:
            raise ValueError("Unexpected type for datetime: {}".format(type(_datetime)))

    def set_log_level(self, log_level: str):
        self.log['actual_level'] = log_level

    def set_log_type(self, log_type: str):
        self.log['logType'] = log_type

    def add_failed_grok_parsing(self, identifier: str):
        if identifier:
            if '_failed_grok_field_parsing' not in self.log:
                self.log['_failed_grok_field_parsing'] = []
            self.log['_failed_grok_field_parsing'].append(identifier)

    def set_message(self, message: str):
        self.log['message'] = message

    def set_prev_timestamp(self, timestamp: str):
        self.log["_prev_timestamp"] = timestamp

    def get_datetime_str(self):
        return self.get_or_none('@timestamp')

    def get_log_level(self):
        return self.get_or_none('actual_level')

    def get_message(self):
        return self.get_or_none('message')

    def get_private_key(self):
        return self.get_or_none('private_key')

    def get_app(self):
        return self.get_or_none('app_name')

    def get_log_type(self):
        return self.get_or_none('log_type')

    def get_or_none(self, key):
        return self.log.get(key, None)

    def contains(self, key: str):
        return key in self.log

    def update(self, fields: dict):
        for key in fields:
            if key not in self.protected_fields:
                self.log[key] = fields[key]
            else:
                logging.debug("Trying to set protected field %s. This operation is refused.", key)

    def map_message_field(self, message_key: str):
        message = self.get_or_none(message_key)
        if message:
            self.set_message(message)

    def unify_log_representation(self):
        # Unify time and log level formats
        self._unify_time_format()
        self._unify_log_level()
        # Exclude all fields that are None
        self.log = dict(filter(lambda item: item[1] is not None, self.log.items()))

    def _unify_time_format(self):
        # Pop all datetime keys from log
        datetime_strings = [self.log.pop(key) for key in self.timestamp_keys if key in self.log]
        if datetime_strings:
            timestamp = None
            for datetime_string in datetime_strings:
                try:
                    timestamp = parser.parse(datetime_string)
                except Exception as e:
                    logging.info("Unable to parse datetime string %s. Reason: %s",
                                 datetime_string, e)
                    continue
                break
            if not timestamp:
                logging.info("Unable to parse datetime strings. Will use current time.")
                timestamp = datetime.datetime.now().replace(microsecond=0)
        else:
            logging.info("No timestamp key found. Will use current time.")
            timestamp = datetime.datetime.now().replace(microsecond=0)

        self.set_datetime(timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f'))

    def _unify_log_level(self):
        levels = self._get_log_severities()
        if levels:
            level = levels[0]
        else:
            level = self._infer_log_level_from_message(self.get_message())

        self.set_log_level(level)

    def _get_log_severities(self):
        levels = []
        for level in [self.log.get(key).lower() for key in self.level_keys if key in self.log]:
            levels.append(self.level_mappings.get(level, level))
        return levels

    def _infer_log_level_from_message(self, message):
        logging.info("Trying to infer log level from log message...")
        level = next((self.level_mappings.get(key) for key in self.level_mappings if key in message),
                     self.default_level)
        logging.info("Inferred log level: %s", level)
        return level

    def to_json_string(self):
        return json.dumps(self.log)
