import json
import os
from typing import Any, Optional

from logsight.connectors import Sink
from logsight.connectors.connectors.file import FileConfigProperties, FileConnector


class FileSink(FileConnector, Sink):
    """Sink that writes to a file."""

    def __init__(self, config: FileConfigProperties):
        super().__init__(config)
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
