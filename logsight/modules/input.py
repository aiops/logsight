import time
from copy import deepcopy
from typing import Any, Optional

from connectors.sources import Source
from modules.core import AbstractHandler, Module


class InputModule(Module, AbstractHandler):
    module_name = "input_module"

    def __init__(self, source: Source, app_settings=None):
        Module.__init__(self)
        AbstractHandler.__init__(self)

        self.source = source

    def handle(self, request: Any) -> Optional[str]:
        return super().handle(request)

    def start(self, ctx: dict):
        ctx["module"] = self.module_name
        super().start(ctx)
        self.source.connect()
        # This is a multiprocessing.Event which notifies the parend process that the connection was established
        while self.source.has_next():
            request = self.source.receive_message()
            self.handle(request)

    def _process_data(self, data: Any) -> Optional[Any]:
        """Not used for input"""
        pass

    def to_json(self):
        d = super().to_json()
        d.update({"source": self.source.to_json()})
        return d
