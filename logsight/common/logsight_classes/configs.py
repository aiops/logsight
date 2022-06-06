from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union


@dataclass
class MetadataConfig:
    input_module: str
    kafka_topics: Optional[List[str]] = field(default_factory=list)


@dataclass
class ModuleConfig:
    classname: str
    args: Optional[Dict] = field(default_factory=dict)
    next_module: Optional[Union[str, List[str]]] = None


@dataclass
class ConnectionConfigProperties:
    classname: str
    connection: str
    params: Dict[str, Any] = field(default_factory=dict)
    serializer: Optional[str] = None


@dataclass
class PipelineConnectors:
    data_source: ConnectionConfigProperties
    control_source: Optional[ConnectionConfigProperties] = None


@dataclass
class PipelineConfig:
    modules: Dict[str, ModuleConfig]
    connectors: PipelineConnectors
    metadata: Optional[Dict] = field(default_factory=dict)
