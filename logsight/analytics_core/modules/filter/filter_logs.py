from typing import List
from functools import reduce

from analytics_core.logs import LogsightLog

conditions = {
    "contains": lambda target, value: value in target,
    "not_contains": lambda target, value: value not in target,
    "equals": lambda target, value: target == value,
    "not_equals": lambda target, value: target != value,
    "not": lambda target, value: value is not target,
    "is": lambda target, value: value is target
}


class Filter:
    def __init__(self, key: str, condition: str, value: str):
        self.fn = conditions[condition]
        self.key = key
        self.value = value

    def filter(self, logs: List[LogsightLog]):
        return list(
            filter(lambda x: self.fn(reduce(lambda a, b: a.get(b, {}), self.key.split("."), x.__dict__), self.value),
                   logs))
