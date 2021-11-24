import threading
from .parsers import Parser

from modules.api import State


def synchronized(func):
    """Makes functions thread-safe"""
    func.__lock__ = threading.Lock()

    def synced_func(*args, **kws):
        with func.__lock__:
            return func(*args, **kws)

    return synced_func


class Status:
    BUFFERING = 0
    PARSED = 1
    MOVE_STATE = 2


class ParserPredictState(State):

    def __init__(self, parser, configs):
        self.configs = configs
        self.parser = parser
        self.parser.set_state(Parser.TEST_STATE)
        self.predict_limit = configs.get('retrain_after')
        self.parse_count = 0

    def process(self, data):
        self.parse_count += 1
        status = Status.PARSED if self.parse_count <= self.predict_limit else Status.MOVE_STATE
        return self.parser.parse(data), status

    def next_state(self):
        return ParserTuneState(self.parser, self.configs)

    def finish_state(self):
        return self.next_state()


class ParserTrainState(State):

    def __init__(self, parser, configs):
        self.parser = parser
        self.parser.set_state(Parser.TRAIN_STATE)

        self.configs = configs
        self.buffer = []
        self.buffer_size = configs.get('buffer_size')

    @synchronized
    def process(self, data):
        result = None

        self.buffer.append(data)

        if len(self.buffer) == self.buffer_size:
            result = self._process_buffer()
            status = Status.MOVE_STATE
        else:
            status = Status.BUFFERING
        return result, status

    def _timeout_call(self):
        self._process_buffer()

    def finish_state(self):
        result = self._process_buffer()
        return result, Status.MOVE_STATE

    @synchronized
    def _process_buffer(self):
        if len(self.buffer) == 0:
            return None, Status.MOVE_STATE
        _ = [self.parser.parse(item) for item in self.buffer]
        self.parser.set_state(Parser.TEST_STATE)
        # parse again after training
        parsed = [self.parser.parse(item) for item in self.buffer]
        self.buffer = []
        return parsed

    def next_state(self):
        return ParserPredictState(self.parser, self.configs)


class ParserTuneState(State):

    def __init__(self, parser, configs):
        self.configs = configs
        self.retrain_size = configs.get('buffer_size')
        self.parser = parser
        self.parse_count = 0
        self.parser.set_state(Parser.TUNE_STATE)

    def process(self, data):
        self.parse_count += 1
        status = Status.PARSED if self.parse_count <= self.retrain_size else Status.MOVE_STATE
        return self.parser.parse(data), status

    def next_state(self):
        return ParserPredictState(self.parser, self.configs)

    def finish_state(self):
        return self.next_state()
