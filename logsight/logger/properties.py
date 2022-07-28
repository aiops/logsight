from os.path import join, split, realpath

from pydantic import BaseModel

from logsight.configs.properties import ConfigProperties


@ConfigProperties(prefix="logger")
class LoggerConfigProperties(BaseModel):
    config_path: str = join(split(realpath(__file__))[0], "log.json")
