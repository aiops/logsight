import json
import os
from pathlib import Path
from typing import Any, Optional

from configs.global_vars import FILE_SINK_PATH
from .sink import Sink


class FileSink(Sink):

    def __init__(self, app_id, path=None, module_name=None, **kwargs):
        super().__init__(**kwargs)
        self.app_id = app_id
        self.folder_path = path or FILE_SINK_PATH
        self.module_name = module_name or ""
        self.file_path = self._create_file_path()
        self.file = None

    def _create_file_path(self):
        file_path = Path(self.folder_path) / ("_".join([self.app_id, self.module_name]) + ".json")
        file_path = Path(os.path.abspath(file_path))
        if not os.path.exists(file_path.parent):
            os.makedirs(file_path.parent)
        return file_path

    def close(self):
        self.file.close()

    def connect(self):
        self.file = open(self.file_path, 'a+')

    def send(self, data: Any, target: Optional[str] = None):
        if not isinstance(data, list):
            data = [data]
            for x in data:
                self.file.write(json.dumps(x) + "\n")
