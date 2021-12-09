from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from json import JSONDecodeError
from typing import Optional, List, Dict, Tuple

from . import Log
from .grok import Grok

logger = logging.getLogger("logsight." + __name__)


class FieldParser(ABC):
    def __init__(self, parser_type: str):
        self.type = parser_type
        self._prev_time = None

    def parse_fields(self, log: Dict) -> Tuple[Dict, bool]:
        # 1. preprocess log
        log_obj = self.__preprocess_message(log)
        log_obj.set_prev_timestamp(self._get_prev_time())
        # 2. Parse log
        parsed_message = self._parse_fields(log_obj.get_message())
        # 3. post process log
        if parsed_message:
            log_obj.update(parsed_message)
        else:
            log_obj.tag_failed_field_parsing(self.type)
        log_obj.unify_log_representation()

        if self._prev_time:  # Set only if it is not None. Otherwise
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
    def __init__(self):
        super().__init__('json')

    def _parse_fields(self, json_str: str) -> Optional[Dict]:
        # Check if the message element in the log is a json by naively parsing it. If it fails, its not a json.
        try:
            log_message_json = json.loads(json_str, strict=False)
        except JSONDecodeError:
            return
        return log_message_json


class GrokParser(FieldParser):
    def __init__(self, log_type: str, grok: Grok):
        super().__init__(log_type)
        self.grok = grok

    def _parse_fields(self, log_message: str) -> Optional[Dict]:
        return self.grok.parse(log_message)


class NoParser(FieldParser):
    def __init__(self):
        super().__init__('no_parser')

    def _parse_fields(self, log_message: str) -> Optional[Dict]:
        return {"message": log_message}
