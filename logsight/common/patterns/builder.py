from abc import ABC, abstractmethod


class BuilderException(Exception):
    """ Base exception that is thrown during building objects"""
    pass


class Builder(ABC):
    """ Base builder class"""

    @abstractmethod
    def build(self, object_config):
        raise NotImplementedError
