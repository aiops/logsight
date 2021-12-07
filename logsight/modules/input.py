from typing import Any, Optional

from connectors.sources import Source
from modules.core import AbstractHandler, Module


class InputModule(Module, AbstractHandler):
    module_name = "input_module"

    def __init__(self, source: Source):
        self.source = source

    def handle(self, **kwargs) -> Optional[str]:
        return self.source.receive_message()

    def start(self):
        super().start()
        while self.source.has_next():
            line = self.handle()
            if self.next_handler:
                self.next_handler.handle(line)

    def _process_data(self, data: Any) -> Optional[Any]:
        """Not used for input"""
        pass

    def to_json(self):
        d = super().to_json()
        d.update({"source": self.source.to_json()})
        return d
