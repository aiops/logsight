from os.path import dirname, join, realpath

from pydantic import BaseModel

from logsight.configs.properties import ConfigProperties


@ConfigProperties(prefix="logger")
class LoggerConfigProperties(BaseModel):
    config_path: str = join(dirname(realpath(__file__)), "log.cfg")
