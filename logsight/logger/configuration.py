import json

from configs.properties import LogsightProperties
from logger.properties import LoggerConfigProperties


class LogConfig:
    def __init__(self):
        log_config = LoggerConfigProperties()
        base_config = LogsightProperties()
        self.config = json.load(open(log_config.config_path, 'r'))
        if base_config.debug:
            self.config['loggers']['logsight']['handlers'] = ["debug", "warning"]
            self.config['loggers']['logsight']['level'] = 'DEBUG'

    def __repr__(self):
        return self.config

    def __getattr__(self, item):
        return self.config.__getattr__(item)
