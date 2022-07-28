from typing import Optional

from pydantic import BaseModel

from logsight.configs.properties import ConfigProperties


@ConfigProperties(prefix="connectors.kafka")
class KafkaConfigProperties(BaseModel):
    host: str
    port: int
    topic: str
    offset: Optional[str] = 'latest'
    group_id: Optional[int]
    auto_commit_interval_ms: Optional[int] = 1000
    max_partition_fetch_bytes: Optional[int] = 5 * 1024 * 1024
    enable_auto_commit: Optional[bool] = True
