from config import Config
from configs.global_vars import CONFIG_PATH

from functools import partial

CONFIG_PATH = "/Users/pilijevski/work/logsight/logsight/logsight/logsight/configs/logsight_config.cfg"


class ConfigProperties(object):
    def __init__(self, prefix=None):
        cfg = Config(CONFIG_PATH)
        self.cfg = cfg.get(prefix, {})

    def __call__(self, cls):
        return partial(cls, **self.cfg)
