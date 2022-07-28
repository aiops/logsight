from typing import Optional

from pydantic import BaseModel

from logsight.configs.properties import ConfigProperties


@ConfigProperties(prefix="connectors.file")
class FileConfigProperties(BaseModel):
    path: str = ""
    mode: Optional[str]
    batch_size: Optional[int]
