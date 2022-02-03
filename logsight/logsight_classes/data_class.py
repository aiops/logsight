from uuid import UUID

from pydantic import BaseModel
from typing import Optional, List, Union, Dict


class AppConfig(BaseModel):
    application_id: UUID
    application_name: str
    private_key: str
    action: str = ""


class MetadataConfig(BaseModel):
    input: str
    kafka_topics: Optional[List[str]] = []


class HandlerConfig(BaseModel):
    args: Optional[dict] = None
    classname: str
    next_handler: Optional[Union[str, List[str]]] = None


class PipelineConfig(BaseModel):
    metadata: MetadataConfig
    handlers: Dict[str, HandlerConfig]
