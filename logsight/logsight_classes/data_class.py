from pydantic import BaseModel
from typing import Optional, List, Union


class MetadataConfig(BaseModel):
    start: str


class HandlerConfig(BaseModel):
    args: Optional[dict] = None
    next_handler: Optional[Union[str, List[str]]] = None


class HandlersConfig(BaseModel):
    field_parsing: Optional[HandlerConfig] = None
    log_parsing: Optional[HandlerConfig] = None
    anomaly_detection: Optional[HandlerConfig] = None
    ad_fork: Optional[HandlerConfig] = None
    ad_sink: Optional[HandlerConfig] = None
    log_aggregation: Optional[HandlerConfig] = None
    agg_sink: Optional[HandlerConfig] = None


class PipelineConfig(BaseModel):
    metadata: MetadataConfig
    handlers: HandlersConfig
