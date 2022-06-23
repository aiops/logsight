from typing import List, Union

from connectors.base.mixins import ConnectableSource


class FileSource(ConnectableSource):

    def _connect(self):
        self.file = open(self.files_list[self.i], 'r')

    def close(self):
        self.file.close()

    def __init__(self, path: Union[str, List[str]], batch_size=None):
        """
        Args:
            path:str: Specify the path to the file or files that will be used by this class
            batch_size=None: Number of lines to read per function call
        """
        self.files_list = path if isinstance(path, list) else [path]
        self.i = self.cnt = 0
        self.batch_size = batch_size
        self.file = None
        self.eof = False

    def receive_message(self) -> Union[str, List[str]]:
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
