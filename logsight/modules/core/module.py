from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, Any, List

from connectors.sinks import Sink
from connectors.sources import Source
from modules.core.wrappers import synchronized


class Module(ABC):
    module_name = "module"

    @abstractmethod
    def _process_data(self, data: Any) -> Optional[Any]:
        raise NotImplementedError

    def to_json(self):
        return {"name": self.module_name}


class ControlModule(Module):
    module_name = "control_module"

    def __init__(self, control_source: Source, control_sink: Sink):
        super().__init__()
        self.control_source = control_source
        self.control_sink = control_sink

    @abstractmethod
    def _process_data(self, data: Any) -> Optional[Any]:
        raise NotImplementedError

    def connect(self):
        self.control_source.connect()
        self.control_sink.connect()


class Subject(ABC):
    """
        This is an implementation of the  observer pattern:
        https://refactoring.guru/design-patterns/observer/python/example
    """
    def __init__(self):
        self._state: Optional[Any] = None
        self._observers: List[SubjectObserver] = []

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value: Subject):
        self._state = value
        self.notify()

    def attach(self, observer: SubjectObserver) -> None:
        self._observers.append(observer)

    def detach(self, observer: SubjectObserver) -> None:
        self._observers.remove(observer)

    @synchronized
    def notify(self) -> None:
        for observer in self._observers:
            observer.on_update(self._state)


class SubjectObserver(ABC):
    """
        This is an implementation of the  observer pattern:
        https://refactoring.guru/design-patterns/observer/python/example
    """

    @abstractmethod
    def on_update(self, state: Subject) -> None:
        raise NotImplementedError


class StatefulControlModule(ControlModule, Subject):
    module_name = "stateful_control_module"

    def __init__(self, control_source: Source, control_sink: Sink):
        super().__init__(control_source, control_sink)

    @abstractmethod
    def _process_data(self, data: Any) -> Optional[Any]:
        raise NotImplementedError
