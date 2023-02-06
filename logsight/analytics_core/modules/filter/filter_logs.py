from functools import reduce
from typing import List

from logsight.analytics_core.logs import LogsightLog

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
        """
        The function takes in a key, a condition, and a value, and then sets the function to the
        condition, and sets the key and value to the key and value.
        
        :param key: The key to check in the dictionary
        :type key: str
        :param condition: The condition to be checked
        :type condition: str
        :param value: The value to compare against
        :type value: str
        """
        self.fn = conditions[condition]
        self.key = key
        self.value = value

    def filter(self, logs: List[LogsightLog]):
        """
        It takes a list of LogsightLog objects, and returns a list of LogsightLog objects that match the
        filter
        
        :param logs: List[LogsightLog]
        :type logs: List[LogsightLog]
        :return: A list of LogsightLog objects
        """
        return list(
            filter(lambda x: self.fn(reduce(lambda a, b: a.get(b, {}), self.key.split("."), x.__dict__), self.value),
                   logs))
