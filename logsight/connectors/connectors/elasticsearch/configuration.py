from typing import Optional

from pydantic import BaseModel

from logsight.configs.properties import ConfigProperties


@ConfigProperties(prefix="connectors.elasticsearch")
class ElasticsearchConfigProperties(BaseModel):
    scheme: str
    host: str
    port: int
    username: str
    password: str
    ingest_pipeline: Optional[str] = None
    max_fetch_size: Optional[int] = 10000