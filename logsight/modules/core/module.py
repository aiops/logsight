from abc import ABC, abstractmethod
from typing import Optional, Any, Callable, List

from connectors.sources import Source
from connectors.sinks import Sink
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


class ControlModuleState(ABC):

    def __init__(self):
        pass


class ControlModuleStateObserver(ABC):
    @abstractmethod
    def on_update(self, state: ControlModuleState) -> None:
        raise NotImplementedError


class StatefulControlModule(ControlModule):
    module_name = "stateful_control_module"

    def __init__(self, control_source: Source, control_sink: Sink):
        super().__init__(control_source, control_sink)
        self._state: Optional[ControlModuleState] = None
        self._observers: List[ControlModuleStateObserver] = []

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value: ControlModuleState):
        self._state = value
        self.notify()

    def attach(self, observer: ControlModuleStateObserver) -> None:
        self._observers.append(observer)

    def detach(self, observer: ControlModuleStateObserver) -> None:
        self._observers.remove(observer)

    @synchronized
    def notify(self) -> None:
        for observer in self._observers:
            observer.on_update(self._state)

    @abstractmethod
    def _process_data(self, data: Any) -> Optional[Any]:
        raise NotImplementedError
