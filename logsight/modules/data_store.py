from typing import Any, Optional

from connectors.sinks import Sink
from modules.core import AbstractHandler, Module


class DataStoreModule(Module, AbstractHandler):
    module_name = "data_store"

    def __init__(self, sink: Sink):
        self.sink = sink

    def _process_data(self, data: Any) -> Optional[Any]:
        if data:
            return self.sink.send(data)

    def handle(self, request: Any) -> Optional[str]:
        return self._process_data(request)

