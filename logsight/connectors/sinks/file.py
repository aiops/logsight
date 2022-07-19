import json
import os
from pathlib import Path
from typing import Any, List, Optional, Union

from connectors.base.mixins import ConnectableSink


class FileSink(ConnectableSink):
    """Sink that writes to a file."""

    def __init__(self, path: Union[str, List[str]], mode: str = "a+"):
        self.path = Path(path)
        self.mode = mode
        self.file = None

    def close(self):
        self.file.close()

    def _connect(self):
        if not os.path.exists(self.path.parent):
            os.makedirs(self.path.parent)
        self.file = open(self.path, self.mode)

    def send(self, data: Any, target: Optional[str] = None):
        if not isinstance(data, list):
            data = [data]
            for x in data:
                self.file.write(json.dumps(x) + "\n")
