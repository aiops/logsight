from abc import ABC, abstractmethod


class Parser(ABC):
    TRAIN_STATE = 0
    TEST_STATE = 1
    TUNE_STATE = 2
    """Abstract class for log parsers. Every log parser should implement the parse method."""

    def __init__(self):
        self.state = self.TRAIN_STATE

    @abstractmethod
    def parse(self, text):
        raise NotImplementedError

    def set_state(self, state):
        if state not in [self.TRAIN_STATE, self.TEST_STATE, self.TUNE_STATE]:
            raise Exception(f"Invalid state {state}")
        self.state = state
