from typing import Any, Optional

from modules.core import Module, ForkHandler


class ForkModule(Module, ForkHandler):
    def __init__(self, config):
        pass

    def _process_data(self, data: Any) -> Optional[Any]:
        pass
