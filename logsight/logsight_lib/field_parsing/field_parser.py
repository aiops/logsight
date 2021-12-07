from __future__ import annotations
from abc import ABC, abstractmethod
import json
from datetime import datetime
from json import JSONDecodeError
from typing import Optional

from .grok import Grok
from . import Log


class FieldParser(ABC):
    def __init__(self, parser_type: str):
        self.type = parser_type
        self._prev_time = datetime.now()

    def parse_fields(self, log_message: dict):
        # 1. preprocess log
        log = self._preprocess_message(log_message)
        # 2. Parse log
        parsed_message = self._parse_fields(log.get_message())
        # 3. post process log
        if parsed_message:
            log.update(parsed_message)
        else:
            log.tag_failed_field_parsing(self.type)
        log.unify_log_representation()

        self._prev_time = log.get_timestamp()

        return log.log

    @abstractmethod
    def _parse_fields(self, message: str):
        raise NotImplementedError

    def _preprocess_message(self, message: dict) -> Log:
        log = Log(message)
        log.set_field_parser_type(self.type)
        log.set_prev_timestamp(self._prev_time)
        return log


class JSONParser(FieldParser):
    def __init__(self):
        super().__init__('json')

    def _parse_fields(self, json_str: str) -> Optional[dict]:
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

    def _parse_fields(self, log_message: str):
        return self.grok.parse(log_message)


class NoParser(FieldParser):
    def __init__(self):
        super().__init__('no_parser')

    def _parse_fields(self, log_message: str):
        return {"message": log_message}
