from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class ConnectorConfigProperties:
    """
    This class is used to define the configuration properties of a connector
    """
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
