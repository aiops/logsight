from pathlib import Path

from connectors import ConnectableConnector
from .configuration import FileConfigProperties


class FileConnector(ConnectableConnector):
    def __init__(self, config: FileConfigProperties):
        self.path = Path(config.path)
        self.mode = config.mode
        self.file = None

    def close(self):
        self.file.close()

    def _connect(self):
        raise NotImplementedError
