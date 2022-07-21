from typing import List, Union

from connectors import Source
from connectors.connectors.file.configuration import FileConfigProperties
from connectors.connectors.file.connector import FileConnector


class FileSource(FileConnector, Source):

    def __init__(self, config: FileConfigProperties = None):
        """
        Args:
           config: File configuration parameters
        """
        super().__init__(config)
        self.files_list = config.path if isinstance(config.path, list) else [config.path]
        self.i = self.cnt = 0
        self.batch_size = config.batch_size
        self.file = None
        self.eof = False

    def _connect(self):
        self.file = open(self.files_list[self.i], 'r')

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
