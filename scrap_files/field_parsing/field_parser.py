from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from json import JSONDecodeError
from typing import Dict, List, Optional, Tuple

from common.logsight_classes import LogEvent
from scrap_files.field_parsing.grok import Grok
from scrap_files.field_parsing.log import Log

logger = logging.getLogger("logsight." + __name__)


class NewFieldParser(ABC):
    def parse_fields(self, log: LogEvent) -> LogEvent:
        self.preprocess_log()
        self._parse_log()
        pass


class FieldParser(ABC):
    def __init__(self, parser_type: str, log_parsing_fail: bool = True):
        self.type = parser_type
        self.log_parsing_fail = log_parsing_fail
        self._prev_time = None

    def parse_fields(self, log: LogEvent) -> Tuple[Optional[Dict], Optional[bool]]:
        log_dict = log.__dict__
        # 1. preprocess log
        log_obj = self.__preprocess_message(log_dict)
        # 2. Parse log
        parsed_message = self._parse_fields(log_obj.get_message())
        # 3. post process log
        if parsed_message:
            log_obj.update(parsed_message)
        else:
            if self.log_parsing_fail:
                logger.debug(f"Failed to parse log {log} with parser {self.type}")
            log_obj.tag_failed_field_parsing(self.type)

        log_obj.unify_log_representation()
        if not log_obj.get_timestamp() and not self._prev_time:
            return None, None
        elif not log_obj.get_timestamp() and self._prev_time:
            log_obj.set_timestamp(self._prev_time)
        elif log_obj.get_timestamp():
            self._prev_time = log_obj.get_timestamp()
            return log_obj.log, parsed_message is not None

    def parse_prev_timestamp(self, logs: List[Dict]):
        for log in logs:
            log_obj = self.__preprocess_message(log)
            parsed_message = self._parse_fields(log_obj.get_message())
            if parsed_message:
                log_obj.update(parsed_message)
            if log_obj.get_timestamp():
                self._prev_time = log_obj.get_timestamp()
                return
        self._prev_time = None

    @abstractmethod
    def _parse_fields(self, message: str) -> Optional[Dict]:
        raise NotImplementedError

    def _get_prev_time(self):
        if self._prev_time:
            return self._prev_time
        else:
            return datetime.utcnow()

    def __preprocess_message(self, log: Dict) -> Log:
        log = Log(log)
        log.set_field_parser_type(self.type)
        return log


class JSONParser(FieldParser):
    def __init__(self, log_parsing_fail: bool = True):
        super().__init__('json', log_parsing_fail)

    def _parse_fields(self, json_str: str) -> Optional[Dict]:
        # Check if the message element in the log is a json by naively parsing it. If it fails, its not a json.
        try:
            log_message_json = json.loads(json_str, strict=False)
        except JSONDecodeError:
            return
        return log_message_json


class GrokParser(FieldParser):
    def __init__(self, log_type: str, grok: Grok, log_parsing_fail: bool = True):
        super().__init__(log_type, log_parsing_fail)
        self.grok = grok

    def _parse_fields(self, log_message: str) -> Optional[Dict]:
        return self.grok.parse(log_message)


class NoParser(FieldParser):
    def __init__(self):
        super().__init__('no_parser')

    def _parse_fields(self, log_message: str) -> Optional[Dict]:
        return {"message": log_message}
