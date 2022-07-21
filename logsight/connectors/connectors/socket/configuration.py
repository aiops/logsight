from typing import Optional

from configs.properties import ConfigProperties
from pydantic import BaseModel


@ConfigProperties("connectors.socket")
class SocketConfigProperties(BaseModel):
    host: str = "localhost"
    port: int = 9992
    max_size: Optional[int] = 2048
