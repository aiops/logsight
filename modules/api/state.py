from abc import ABC, abstractmethod


class State(ABC):
    @abstractmethod
    def process(self, task):
        raise NotImplementedError
