from copy import deepcopy
from typing import Any, Optional

from modules.core import Module, ForkHandler


class ForkModule(Module, ForkHandler):
    module_name = "fork_module"
    """Only serves as an interface to be compatible with other modules, it is implemented in fork handler"""

    def __init__(self, config, app_settings=None):
        Module.__init__(self)
        ForkHandler.__init__(self)

        self.config = config

    def start(self, ctx: dict):
        ctx["module"] = self.module_name
        super().start(ctx)

    def _process_data(self, data: Any) -> Optional[Any]:
        """Implemented in fork handler"""
        pass
