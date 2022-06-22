from typing import List, Optional, Union

from .source import Source
from ..serializers import Serializer


class StdinSource(Source):
    def _receive_message(self) -> str:
        txt = input("[SOURCE] Enter message: ")
        return txt


class FileSource(Source):

    def __init__(self, path: Union[str, List[str]], batch_size=None, serializer: Optional[Serializer] = None):
        super().__init__()
        self.files_list = path if isinstance(path, list) else [path]
        self.i = self.cnt = 0
        self.batch_size = batch_size
        self.file = open(self.files_list[self.i], 'r')
        self.eof = False

    def _receive_message(self) -> Union[str, List[str]]:
        if self.batch_size:
            batch = []
            for _ in range(self.batch_size):
                line = self._read_line()
                if line:
                    batch.append(line)
                else:
                    break
            return batch
        return self._read_line()

    def _read_line(self):
        line = self.file.readline()
        self.cnt += 1
        if line == "":
            return self._reopen_file()
        return line

    def _reopen_file(self):
        self.i += 1
        if self.i == len(self.files_list):
            self.eof = True
            return
        self.file.close()
        self.file = open(self.files_list[self.i], 'r')
        return self.receive_message()

    def has_next(self):
        return not self.eof
