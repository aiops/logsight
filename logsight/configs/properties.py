from config import Config
from pydantic import BaseModel
from functools import partial

from configs.global_vars import CONFIG_PATH


class ConfigProperties(object):
    def __init__(self, prefix="base", path=CONFIG_PATH):
        cfg = Config(str(path))
        self.cfg = cfg.get(prefix, cfg.as_dict())

    def __call__(self, cls):
        return partial(cls, **self.cfg)


@ConfigProperties(prefix='logsight')
class LogsightProperties(BaseModel):
    debug: bool = False
    retry_attempts: int = 6
    retry_timeout: int = 10
