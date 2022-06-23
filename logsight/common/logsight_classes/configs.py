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
class ConnectorConfigProperties:
    connector_type: str
    connection: str
    params: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.connector_type not in ['source', 'sink']:
            raise ValueError("Field 'connector_type' must be one of 'sink','source'.")


@dataclass
class AdapterConfigProperties:
    connector: ConnectorConfigProperties
    serializer: Optional[str] = None


@dataclass
class PipelineConnectors:
    data_source: AdapterConfigProperties
    control_source: Optional[AdapterConfigProperties] = None


@dataclass
class PipelineConfig:
    modules: Dict[str, ModuleConfig]
    connectors: PipelineConnectors
    metadata: Optional[Dict] = field(default_factory=dict)
