from dataclasses import dataclass, field
from typing import Dict, List, Optional
from uuid import uuid4


@dataclass
class LogEvent:
    message: str
    timestamp: Optional[str] = None
    level: Optional[str] = None


@dataclass
class LogsightLog:
    event: LogEvent
    id: Optional[str] = ""
    metadata: Optional[Dict] = field(default_factory=dict)
    # TODO: Should the results from the pipeline go into a separate attribute


@dataclass
class LogBatch:
    logs: List[LogsightLog]
    id: Optional[str] = str(uuid4())
    metadata: Optional[Dict] = field(default_factory=dict)
