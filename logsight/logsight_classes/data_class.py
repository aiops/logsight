from pydantic import BaseModel
from typing import Optional, List, Union, Dict


class AppConfig(BaseModel):
    application_id: str
    private_key: str
    user_name: Optional[str] = None
    application_name: str
    status: Optional[str] = None


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
