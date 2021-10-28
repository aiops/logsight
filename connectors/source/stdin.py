import json
from time import sleep

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
        self.file = open(self.path, 'r')
        self.eof = False

    def receive_message(self):
        txt = self.file.readline()
        if txt == "":
            self.eof = True
            return
        sleep(0.2)
        txt = json.loads(txt)
        if "@timestamp" in txt:
            return txt
        else:
            return self.receive_message()

    def has_next(self):
        return not self.eof

    def process_message(self):
        pass
