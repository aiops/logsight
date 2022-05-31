from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Union

from analytics_core.logs import LogBatch, LogsightLog
from common.patterns.chain_of_responsibility import AbstractHandler
from connectors import Connector, Sink, Source
from pipeline.modules.core.transformer import DataTransformer


class BaseModule(ABC):
    """A base class for all modules"""
    _transform_function: Callable[[LogsightLog], LogsightLog] = None
    _module_name = __name__

    @property
    def name(self):
        return self._module_name

    @name.setter
    def name(self, name):
        self._module_name = name

    def __repr__(self):
        return f"{self.__class__.__name__}: {self._module_name}"

    @abstractmethod
    def process(self, batch: LogBatch) -> LogBatch:
        raise NotImplementedError


class Module(BaseModule, AbstractHandler, ABC):

    def __init__(self):
        BaseModule.__init__(self)
        AbstractHandler.__init__(self)

    def _handle(self, context: LogBatch) -> LogBatch:
        """TODO: Add documentation. Explain what this method is for"""
        return self.process(context)


class TransformModule(Module, DataTransformer, ABC):

    def process(self, batch: LogBatch) -> LogBatch:
        return self.transform(batch)


class ConnectableModule(Module, ABC):
    def __init__(self, connector: Union[Source, Sink, Connector]):
        super().__init__()
        self.connector = connector
