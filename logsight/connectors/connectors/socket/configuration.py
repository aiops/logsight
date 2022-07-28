from typing import Optional

from pydantic import BaseModel

from logsight.configs.properties import ConfigProperties


@ConfigProperties("connectors.socket")
class SocketConfigProperties(BaseModel):
    host: str = "localhost"
    port: int = 9992
    max_size: Optional[int] = 2048
