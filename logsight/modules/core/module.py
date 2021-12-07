from abc import ABC, abstractmethod
from typing import Optional, Any

from connectors.sources import Source
from connectors.sinks import Sink


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
        self.control_source = control_source
        self.control_sink = control_sink
        self.module_name = "module"

    @abstractmethod
    def _process_data(self, data: Any) -> Optional[Any]:
        raise NotImplementedError
