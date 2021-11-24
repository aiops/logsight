from abc import ABC, abstractmethod


class State(ABC):
    @abstractmethod
    def process(self, task):
        raise NotImplementedError

    @abstractmethod
    def next_state(self):
        raise NotImplementedError
