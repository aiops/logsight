import logging.config
from typing import List

from analytics_core.modules.log_aggregation import LogAggregator
from jobs.common.index_job import IndexJob
from jobs.persistence.dto import IndexInterval

logger = logging.getLogger("logsight." + __name__)


class CalculateLogAggregationJob(IndexJob):
    def __init__(self, index_interval: IndexInterval, **kwargs):
        super().__init__(index_interval, index_ext="log_agg", **kwargs)
        self.log_aggregator = LogAggregator()
        self.index_ext = "log_agg"

    def _calculate(self, logs) -> List:
        return self.log_aggregator.aggregate_logs(logs)
