import json

from .base import Sink
from config.global_vars import FILE_SINK_PATH
from pathlib import Path
import os


class FileSink(Sink):
    def __init__(self, app_id, path=None, module_name=None, **kwargs):
        super().__init__(**kwargs)
        self.app_id = app_id
        self.folder_path = path or FILE_SINK_PATH
        self.module_name = module_name or ""
        self.file_path = self._create_file_path()

    def _create_file_path(self):
        file_path = Path(self.folder_path) / ("_".join([self.app_id, self.module_name]) + ".json")
        file_path = Path(os.path.abspath(file_path))
        if not os.path.exists(file_path.parent):
            os.makedirs(file_path.parent)
        return file_path

    def send(self, data):
        if not isinstance(data, list):
            data = [data]

        with open(self.file_path, 'a+') as f:

            for x in data:
                f.write(json.dumps(x) + "\n")
        # self.file.writelines([json.dumps(x) + "\n" for x in data])
