from dataclasses import dataclass, field
from typing import Dict, List, Optional
from uuid import uuid4


@dataclass
class LogsightLog:
    message: str
    timestamp: str
    level: str
    id: Optional[str] = ""
    metadata: Optional[Dict] = field(default_factory=dict)
    tags: Optional[Dict[str, str]] = field(default_factory=dict)

    def __post_init__(self):
        if not self.metadata:
            self.metadata = dict()
        self.tags.update({"default": "default"}) if len(self.tags) == 0 else self.tags


@dataclass
class LogBatch:
    logs: List[LogsightLog]
    index: str
    id: Optional[str] = str(uuid4())
    metadata: Optional[Dict] = field(default_factory=dict)
