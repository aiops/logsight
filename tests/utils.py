import random
from datetime import datetime

from logsight.analytics_core.logs import LogBatch, LogsightLog
from logsight.analytics_core.modules.log_parsing.mask_parser import ExtractedParameter


class TestInputConfig:
    default_msg = "Hello world 123"
    default_template = "Hello world <:NUM:>"
    default_parameter = ExtractedParameter("123", "NUM")
    default_timestamp = "2021-03-23T01:02:51.007"
    default_level = "INFO"
    logsight_log = LogsightLog(default_msg, default_timestamp, default_level)

    default_index = "test_index"
    default_num_logs = 100
    logsight_logs = [logsight_log] * default_num_logs
    log_batch = LogBatch(logsight_logs, default_index)


