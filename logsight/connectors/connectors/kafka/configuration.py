from typing import Optional

from pydantic import BaseModel

from configs.properties import ConfigProperties


@ConfigProperties(prefix="connectors.kafka")
class KafkaConfigProperties(BaseModel):
    host: str
    port: int
    topic: str
    offset: Optional[str] = 'latest'
    group_id: Optional[int]
