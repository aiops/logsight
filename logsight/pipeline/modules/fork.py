from typing import Optional

from analytics_core.logs import LogBatch
from common.patterns.chain_of_responsibility import ForkHandler
from pipeline.modules.core.module import BaseModule


class ForkModule(BaseModule, ForkHandler):
    """ ForkModule splits the input into multiple outputs."""

    def __init__(self):
        BaseModule.__init__(self)
        ForkHandler.__init__(self)

    def process(self, batch: LogBatch) -> Optional[LogBatch]:
        """The splitting is implemented by the ForkHandler so this function only returns the data."""
        return batch

    def _handle(self, context: LogBatch) -> LogBatch:
        return self.process(context)
