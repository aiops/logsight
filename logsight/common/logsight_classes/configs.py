from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union


@dataclass
class AppConfig:
    application_id: str
    application_name: str
    private_key: str
    action: str = ""

    def __post_init__(self):
        assert len(self.application_name) > 0, "Application name must not be an empty string."
        assert len(self.private_key) > 0, "Private key must not be an empty string."


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
