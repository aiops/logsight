from typing import Callable

from analytics_core.logs import LogsightLog
from pipeline.modules.core import Module


class TestModule(Module):
    def __init__(self):
        super().__init__()

    def _get_transform_function(self) -> Callable[[LogsightLog], LogsightLog]:
        return print_line


def print_line(data: LogsightLog) -> LogsightLog:
    # print(data)
    return data
