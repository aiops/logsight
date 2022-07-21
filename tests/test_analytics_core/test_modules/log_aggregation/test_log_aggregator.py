from analytics_core.modules.log_aggregation import LogAggregator
from logsight.tests.inputs import agg_results, processed_logs


def test_aggregate_logs():
    log_agg = LogAggregator()
    result = log_agg.aggregate_logs(processed_logs)
    assert result == agg_results
