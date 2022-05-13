from analytics_core.logs import LogBatch, LogsightLog
from pipeline.modules.core import TransformModule


class TestModule(TransformModule):
    def transform(self, data: LogBatch) -> LogBatch:
        return data

    def __init__(self):
        super().__init__()


def print_line(data: LogsightLog) -> LogsightLog:
    return data
