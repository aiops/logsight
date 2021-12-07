from typing import Any, Optional

from modules.core import Module, ForkHandler


class ForkModule(Module, ForkHandler):
    def __init__(self, config):
        """Only serves as an intefrace to be compatible with other modules, it is implemented in fork handler"""
        pass

    def _process_data(self, data: Any) -> Optional[Any]:
        """Implemented in fork handler"""
        pass
