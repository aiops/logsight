from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from uuid import uuid4


@dataclass
class LogEvent:
    message: str
    timestamp: str
    level: str


@dataclass
class LogsightLog:
    event: LogEvent
    id: Optional[str] = ""
    metadata: Optional[Dict] = field(default_factory=dict)
    tags: Optional[Set[str]] = field(default_factory=set)

    # TODO: Should the results from the pipeline go into a separate attribute
    def __post_init__(self):
        self.tags.update(["default"]) if len(self.tags) == 0 else self.tags


@dataclass
class LogBatch:
    logs: List[LogsightLog]
    store_index: str
    id: Optional[str] = str(uuid4())
    metadata: Optional[Dict] = field(default_factory=dict)
