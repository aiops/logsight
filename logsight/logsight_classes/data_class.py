from uuid import UUID

from pydantic import BaseModel, validator
from typing import Optional, List, Union, Dict


class AppConfig(BaseModel):
    application_id: str
    application_name: str
    private_key: str
    action: str = ""

    @validator('application_name')
    def application_name_not_empty(cls, v):
        assert len(v) > 0, "Application name must not be an empty string."
        return v

    @validator('private_key')
    def private_key_not_empty(cls, v):
        assert len(v) > 0, "Private key must not be an empty string."
        return v


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
