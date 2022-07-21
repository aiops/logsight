from typing import Optional

from configs.properties import ConfigProperties
from pydantic import BaseModel


@ConfigProperties(prefix="connectors.elasticsearch")
class ElasticsearchConfigProperties(BaseModel):
    scheme: str
    host: str
    port: int
    username: str
    password: str
    ingest_pipeline: Optional[str] = None


if __name__ == '__main__':
    es = ElasticsearchConfigProperties
    print(es)
