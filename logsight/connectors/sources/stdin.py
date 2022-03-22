import json
from time import time

from .source import Source


class PrintSource(Source):

    def close(self):
        pass

    def connect(self):
        return

    def receive_message(self):
        txt = input("[SOURCE] Enter message: ")
        return {"type": txt, "app_name": "test", "version": None}


class FileSource(Source):
    def __init__(self, path=None, **kwargs):
        self.path = path
        files_list = [self.path]
        self.files_list = files_list
        self.i = 0
        self.file = open(self.files_list[self.i], 'r')
        self.eof = False
        self.cnt = 0
        self.time = time()

    def close(self):
        pass

    def connect(self):
        return

    def receive_message(self):
        try:
            txt = json.loads(self.file.readline())
            source = "DEF"
        except Exception:
            txt = self.file.readline()
            source = "DEF"

        self.cnt += 1
        if txt == "":
            return self._reopen_file()
        if self.cnt % 10000 == 0:
            print("Sending", self.cnt, time() - self.time)
        return {"application_id": "test", "app_name": "test", "message": txt, "private_key": "sample_key", "source": source}

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
