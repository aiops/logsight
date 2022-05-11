import logging

from analytics_core.logs import LogBatch
from analytics_core.modules.log_aggregation import LogAggregator
from pipeline.modules.core import Module

logger = logging.getLogger("logsight." + __name__)


class LogAggregationModule(Module):
    def __init__(self):
        super(LogAggregationModule, self).__init__()
        self.aggregator = LogAggregator()

    def process(self, batch: LogBatch) -> LogBatch:
        return self.aggregator.aggregate_logs(batch)
