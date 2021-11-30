import json
import os
from time import sleep, time

from .base import Source


class PrintSource(Source):

    def connect(self):
        return

    def receive_message(self):
        txt = input("[SOURCE] Enter message: ")
        return {"type": txt, "app_name": "test", "version": None}


class FileSource(Source):
    def connect(self):
        return

    def __init__(self, path=None, **kwargs):
        super().__init__(**kwargs)
        self.path = path or "/home/petar/work/logsight/log-monolith/logfile.txt"
        files_list = []
        for root, folders, files in os.walk("/home/petar/work/logsight/data/test_log_dir"):
            for f in files:
                if "syslog" in f:
                    files_list.append("/".join([str(root), str(f)]))
        self.files_list = files_list
        self.i = 0
        self.file = open(self.files_list[self.i], 'r')
        self.eof = False
        self.cnt = 0
        self.time = time()

    def receive_message(self):
        try:
            txt = self.file.readline()
        except UnicodeDecodeError:
            txt = self.file.readline()

        self.cnt += 1
        if txt == "":
            return self._reopen_file()
        if self.cnt % 1000 == 0:
            print("Sending", self.cnt, time() - self.time)
        return {"app_name": "sample_app", "message": txt, "private_key": "sample_key"}

    def _reopen_file(self):
        print('REOPENING FILE!!!!')
        self.i += 1
        if self.i == len(self.files_list):
            self.eof = True
            return
        self.file.close()
        print(self.files_list[self.i])
        self.file = open(self.files_list[self.i], 'r')
        return self.receive_message()

    def has_next(self):
        return not self.eof
