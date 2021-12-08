from typing import Any, Optional

from modules.core import Module, ForkHandler


class ForkModule(Module, ForkHandler):
    def __init__(self, config,app_settings=None):
        """Only serves as an interface to be compatible with other modules, it is implemented in fork handler"""
        self.config = config

    def _process_data(self, data: Any) -> Optional[Any]:
        """Implemented in fork handler"""
        pass
