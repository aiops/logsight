import json
import os
from pathlib import Path
from typing import Any, List, Optional, Union

from .sink import ConnectableSink
from ..serializers import Serializer


class FileSink(ConnectableSink):

    def __init__(self, path: Union[str, List[str]], serializer: Optional[Serializer] = None):
        super().__init__(serializer=serializer)
        self.path = Path(path)
        self.file = None

    def close(self):
        self.file.close()

    def connect(self):
        ensure_path(self.path)
        self.file = open(self.path, 'a+')

    def send(self, data: Any, target: Optional[str] = None):
        if not isinstance(data, list):
            data = [data]
            for x in data:
                self.file.write(json.dumps(x) + "\n")


def ensure_path(path):
    if not os.path.exists(path.parent):
        os.makedirs(path.parent)
    return path
