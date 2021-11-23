import json
import os
from time import sleep, time

from .base import Source


class PrintSource(Source):
    # def receive_message(self):
    #     txt = input("[SOURCE] Enter message: ")
    #     return {"message": txt}

    def receive_message(self):
        txt = input("[SOURCE] Enter message: ")
        return {"type": txt, "app_name": "test", "version": None}

    def process_message(self):
        pass


class FileSource(Source):
    def connect(self):
        pass

    # def receive_message(self):
    #     txt = input("[SOURCE] Enter message: ")
    #     return {"message": txt}
    def __init__(self, path=None, **kwargs):
        super().__init__()
        self.path = path or "/home/petar/work/logsight/log-monolith/logfile.txt"
        files_list = []
        for root, folders, files in os.walk("/home/petar/work/logsight/data/test_log_dir"):
            for f in files:
                files_list.append("/".join([str(root), str(f)]))
        self.files_list = files_list
        self.i = 0
        self.file = open(self.files_list[self.i], 'r')
        self.eof = False
        self.cnt = 0
        self.time = time()

    def receive_message(self):
        txt = self.file.readline()
        sleep(0.05)
        self.cnt += 1
        if txt == "":
            return self._reopen_file()
        if self.cnt % 1000 == 0:
            print("Processed", self.cnt, time() - self.time)
        return {"app_name": "sample_app", "message": txt, "private_key": "sample_key"}

    def _reopen_file(self):
        self.i += 1
        if self.i == len(self.files_list):
            self.eof = True
            return
        self.file.close()
        self.file = open(self.files_list[self.i], 'r')
        print(self.files_list[self.i])
        return self.receive_message()

    def has_next(self):
        return not self.eof

    def process_message(self):
        pass
