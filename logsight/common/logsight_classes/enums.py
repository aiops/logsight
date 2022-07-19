from enum import Enum


class LogBatchStatus(Enum):
    PROCESSING = 0
    DONE = 1
    FAILED = 2
