import os

from pydantic import BaseModel

from logsight.configs.properties import ConfigProperties


@ConfigProperties(prefix="pipeline")
class LoggerConfigProperties(BaseModel):
    config_path: str = os.path.join(os.path.split(os.path.realpath(__file__))[0], "log.json")
