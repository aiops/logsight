import logging
from copy import deepcopy
from typing import Any, Optional

from connectors.sinks import Sink
from modules.core import AbstractHandler, Module

logger = logging.getLogger("logsight." + __name__)


class DataStoreModule(Module, AbstractHandler):
    module_name = "data_store"

    def __init__(self, sink: Sink, app_settings=None):
        Module.__init__(self)
        AbstractHandler.__init__(self)

        self.sink = sink

    def start(self, ctx: dict):
        ctx["module"] = self.module_name
        super().start(ctx)

    def _process_data(self, data: Any) -> Optional[Any]:
        if data:
            return self.sink.send(data)

    def _handel(self, request: Any) -> Optional[str]:
        return self._process_data(request)

    def flush(self, context=None) -> Optional[str]:
        result = self._process_data(context)
        return super().flush(result)
