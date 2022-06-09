import logging

from analytics_core.logs import LogBatch
from analytics_core.modules.log_aggregation import LogAggregator
from common.utils.helpers import to_flat_dict
from pipeline.modules.core import TransformModule

logger = logging.getLogger("logsight." + __name__)


class LogAggregationModule(TransformModule):

    def __init__(self):
        super().__init__()
        self.aggregator = LogAggregator()

    def transform(self, data: LogBatch) -> LogBatch:
        data.metadata['agg'] = self.aggregator.aggregate_logs([to_flat_dict(log) for log in data.logs])
        return data
