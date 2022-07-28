import os
from functools import partial

from config import Config
from pydantic import BaseModel

from logsight.configs.global_vars import CONFIG_PATH


class ConfigProperties(object):
    def __init__(self, prefix="base", path=os.environ.get('CONFIG_PATH', CONFIG_PATH)):
        cfg = Config(str(path))
        self.cfg = cfg.get(prefix, cfg.get(".".join(["logsight", prefix]), {}))

    def __call__(self, cls):
        return partial(cls, **self.cfg)


@ConfigProperties(prefix='logsight')
class LogsightProperties(BaseModel):
    debug: bool = True
    retry_attempts: int = 6
    retry_timeout: int = 10
    pipeline_index_ext: str = "pipeline"
